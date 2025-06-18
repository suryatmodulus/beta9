package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"

	pb "github.com/beam-cloud/beta9/proto"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/go-runc"
	"github.com/google/shlex"
	"github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc"
)

type ContainerServer struct {
	containerRuntime ContainerRuntime
	baseConfigSpec   specs.Spec
	pb.UnimplementedContainerServiceServer
	containerInstances      *common.SafeMap[*ContainerInstance]
	containerRepoClient     pb.ContainerRepositoryServiceClient
	containerNetworkManager *ContainerNetworkManager
	imageClient             *ImageClient
	port                    int
	podAddr                 string
}

func NewContainerServer(containerRuntime ContainerRuntime, podAddr string, containerInstances *common.SafeMap[*ContainerInstance], imageClient *ImageClient, containerRepoClient pb.ContainerRepositoryServiceClient, containerNetworkManager *ContainerNetworkManager) (*ContainerServer, error) {
	var baseConfigSpec specs.Spec
	specTemplate := strings.TrimSpace(string(baseRuncConfigRaw))
	err := json.Unmarshal([]byte(specTemplate), &baseConfigSpec)
	if err != nil {
		return nil, err
	}

	return &ContainerServer{
		podAddr:                 podAddr,
		containerRuntime:        containerRuntime,
		baseConfigSpec:          baseConfigSpec,
		containerInstances:      containerInstances,
		imageClient:             imageClient,
		containerRepoClient:     containerRepoClient,
		containerNetworkManager: containerNetworkManager,
	}, nil
}

func (s *ContainerServer) Start() error {
	listener, err := net.Listen("tcp", ":0") // // Random free port
	if err != nil {
		log.Error().Err(err).Msg("failed to listen")
		os.Exit(1)
	}

	s.port = listener.Addr().(*net.TCPAddr).Port
	log.Info().Int("port", s.port).Msg("runc server started")

	grpcServer := grpc.NewServer()
	pb.RegisterContainerServiceServer(grpcServer, s)

	go func() {
		err := grpcServer.Serve(listener)
		if err != nil {
			log.Error().Err(err).Msg("failed to start grpc server")
			os.Exit(1)
		}
	}()

	return nil
}

func (s *ContainerServer) ContainerKill(ctx context.Context, in *pb.ContainerKillRequest) (*pb.ContainerKillResponse, error) {
	_ = s.containerRuntime.Kill(ctx, in.ContainerId, int(syscall.SIGTERM), &ContainerKillOpts{
		All: true,
	})

	err := s.containerRuntime.Delete(ctx, in.ContainerId, &ContainerDeleteOpts{
		Force: true,
	})

	return &pb.ContainerKillResponse{
		Ok: err == nil,
	}, nil
}

// Execute an arbitary command inside a running container
func (s *ContainerServer) ContainerExec(ctx context.Context, in *pb.ContainerExecRequest) (*pb.ContainerExecResponse, error) {
	cmd := fmt.Sprintf("bash -c '%s'", in.Cmd)
	parsedCmd, err := shlex.Split(cmd)
	if err != nil {
		return &pb.ContainerExecResponse{}, err
	}

	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerExecResponse{Ok: false}, nil
	}

	process := s.baseConfigSpec.Process
	process.Args = parsedCmd
	process.Cwd = instance.Spec.Process.Cwd

	instanceSpec := instance.Spec.Process
	process.Env = append(instanceSpec.Env, in.Env...)

	if instance.Request.IsBuildRequest() {
		/*
			For some reason, if the process that is spun up from this (e.g. `runc exec --process /tmp/runc-process839505971 build-128a153e`)
			is canceled by the context (or just exits), the runc container can no longer be shut down by a `runc kill` command or
			a SIGKILL on the first process.

			For example in build containers, we do a `tail -f /dev/null` to keep the container running. But if the scenario above happens,
			the `tail` process does receive and act on a shut down signal but it is then `blocked and waiting` on some unknown channel to resolve
			which prevents the container from being shut down.
		*/
		ctx = context.Background()
		process.Env = append(process.Env, instance.Request.BuildOptions.BuildSecrets...)
	}

	err = s.containerRuntime.Exec(ctx, in.ContainerId, *process, &ContainerExecOpts{
		OutputWriter: instance.OutputWriter,
	})

	return &pb.ContainerExecResponse{
		Ok: err == nil,
	}, nil
}

func (s *ContainerServer) ContainerStatus(ctx context.Context, in *pb.ContainerStatusRequest) (*pb.ContainerStatusResponse, error) {
	state, err := s.containerRuntime.State(ctx, in.ContainerId)
	if err != nil {
		return &pb.ContainerStatusResponse{
			Running: false,
		}, nil
	}

	return &pb.ContainerStatusResponse{
		Running: state.Status == types.RuncContainerStatusRunning,
	}, nil
}

func (s *ContainerServer) ContainerStreamLogs(req *pb.ContainerStreamLogsRequest, stream pb.ContainerService_ContainerStreamLogsServer) error {
	instance, exists := s.containerInstances.Get(req.ContainerId)
	if !exists {
		return errors.New("container not found")
	}

	buffer := make([]byte, 4096)
	logEntry := &pb.ContainerLogEntry{}

	for {
		select {
		case <-stream.Context().Done():
			return errors.New("context cancelled")
		default:
		}

		n, err := instance.LogBuffer.Read(buffer)
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		if n > 0 {
			logEntry.Msg = string(buffer[:n])
			if err := stream.Send(logEntry); err != nil {
				return err
			}

			continue
		}

		time.Sleep(time.Duration(100) * time.Millisecond)
	}

	return nil
}

func (s *ContainerServer) ContainerArchive(req *pb.ContainerArchiveRequest, stream pb.ContainerService_ContainerArchiveServer) error {
	ctx := stream.Context()
	state, err := s.containerRuntime.State(ctx, req.ContainerId)
	if err != nil {
		return stream.Send(&pb.ContainerArchiveResponse{Done: true, Success: false, ErrorMsg: "Container not found"})
	}

	if state.Status != types.RuncContainerStatusRunning {
		return stream.Send(&pb.ContainerArchiveResponse{Done: true, Success: false, ErrorMsg: "Container not running"})
	}

	instance, exists := s.containerInstances.Get(req.ContainerId)
	if !exists {
		return stream.Send(&pb.ContainerArchiveResponse{Done: true, Success: false, ErrorMsg: "Container not found"})
	}

	// Copy initial config file from the base image bundle
	err = copyFile(filepath.Join(instance.BundlePath, specBaseName), filepath.Join(instance.Overlay.TopLayerPath(), initialSpecBaseName))
	if err != nil {
		return stream.Send(&pb.ContainerArchiveResponse{Done: true, Success: false, ErrorMsg: err.Error()})
	}

	if err := s.addRequestEnvToInitialSpec(instance); err != nil {
		return err
	}

	tempConfig := s.baseConfigSpec
	tempConfig.Hooks.Prestart = nil
	tempConfig.Process.Terminal = false
	tempConfig.Process.Args = []string{"tail", "-f", "/dev/null"}
	tempConfig.Root.Readonly = false

	file, err := json.MarshalIndent(tempConfig, "", "  ")
	if err != nil {
		return stream.Send(&pb.ContainerArchiveResponse{Done: true, Success: false, ErrorMsg: err.Error()})
	}

	configPath := filepath.Join(instance.Overlay.TopLayerPath(), specBaseName)
	err = os.WriteFile(configPath, file, 0644)
	if err != nil {
		return stream.Send(&pb.ContainerArchiveResponse{Done: true, Success: false, ErrorMsg: err.Error()})
	}

	progressChan := make(chan int)
	doneChan := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		lastProgress := -1

		for {
			select {
			case <-ctx.Done():
				return
			case progress, ok := <-progressChan:
				if !ok {
					return
				}
				if progress > lastProgress && progress != lastProgress {
					lastProgress = progress

					log.Info().Int("progress", progress).Msg("image upload progress")
					err := stream.Send(&pb.ContainerArchiveResponse{Done: false, Success: false, Progress: int32(progress), ErrorMsg: ""})
					if err != nil {
						return
					}
				}
			case <-doneChan:
				return
			}
		}
	}()

	defer func() {
		close(progressChan)
		wg.Wait()
	}()

	topLayerPath := NewPathInfo(instance.Overlay.TopLayerPath())
	err = stream.Send(&pb.ContainerArchiveResponse{
		Done: true, Success: s.imageClient.Archive(ctx, topLayerPath, req.ImageId, progressChan) == nil,
	})

	close(doneChan)
	return err
}

func (s *ContainerServer) addRequestEnvToInitialSpec(instance *ContainerInstance) error {
	if len(instance.Request.Env) == 0 {
		return nil
	}

	specPath := filepath.Join(instance.Overlay.TopLayerPath(), initialSpecBaseName)

	bytes, err := os.ReadFile(specPath)
	if err != nil {
		return err
	}

	var spec specs.Spec
	if err = json.Unmarshal(bytes, &spec); err != nil {
		return err
	}

	spec.Process.Env = append(instance.Request.Env, spec.Process.Env...)

	bytes, err = json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return err
	}

	if err = os.WriteFile(specPath, bytes, 0644); err != nil {
		return err
	}

	return nil
}

func (s *ContainerServer) ContainerSyncWorkspace(ctx context.Context, in *pb.SyncContainerWorkspaceRequest) (*pb.SyncContainerWorkspaceResponse, error) {
	_, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.SyncContainerWorkspaceResponse{Ok: false}, nil
	}

	workspacePath := types.TempContainerWorkspace(in.ContainerId)
	destPath := path.Join(workspacePath, in.Path)
	destNewPath := path.Join(workspacePath, in.NewPath)

	switch in.Op {
	case pb.SyncContainerWorkspaceOperation_DELETE:
		if err := os.RemoveAll(destPath); err != nil {
			return &pb.SyncContainerWorkspaceResponse{Ok: false}, nil
		}
	case pb.SyncContainerWorkspaceOperation_WRITE:
		if in.IsDir {
			os.MkdirAll(destPath, 0755)
		} else {
			os.MkdirAll(path.Dir(destPath), 0755)
			if err := os.WriteFile(destPath, in.Data, 0644); err != nil {
				return &pb.SyncContainerWorkspaceResponse{Ok: false}, nil
			}
		}
	case pb.SyncContainerWorkspaceOperation_MOVED:
		os.MkdirAll(path.Dir(destNewPath), 0755)
		if err := os.Rename(destPath, destNewPath); err != nil {
			return &pb.SyncContainerWorkspaceResponse{Ok: false}, nil
		}
	}

	return &pb.SyncContainerWorkspaceResponse{Ok: true}, nil
}

func (s *ContainerServer) waitForContainer(ctx context.Context, containerId string) error {
	for {
		instance, exists := s.containerInstances.Get(containerId)
		if !exists {
			return errors.New("container not found")
		}

		if instance.Spec == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		state, err := s.containerRuntime.State(ctx, containerId)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if state.Pid != 0 && state.Status == types.RuncContainerStatusRunning {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

func (s *ContainerServer) ContainerSandboxExec(ctx context.Context, in *pb.ContainerSandboxExecRequest) (*pb.ContainerSandboxExecResponse, error) {
	log.Info().Str("container_id", in.ContainerId).Str("cmd", in.Cmd).Msg("running sandbox command")

	parsedCmd, err := shlex.Split(in.Cmd)
	if err != nil {
		return &pb.ContainerSandboxExecResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxExecResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err = s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.ContainerSandboxExecResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	process := s.baseConfigSpec.Process
	process.Args = parsedCmd
	process.Cwd = instance.Spec.Process.Cwd

	if in.Cwd != "" {
		process.Cwd = in.Cwd
	}

	formattedEnv := []string{}
	for key, value := range in.Env {
		formattedEnv = append(formattedEnv, fmt.Sprintf("%s=%s", key, value))
	}

	process.Env = append(instance.Spec.Process.Env, formattedEnv...)
	return s.handleSandboxExec(ctx, in, instance, process)
}

func (s *ContainerServer) handleSandboxExec(ctx context.Context, in *pb.ContainerSandboxExecRequest, instance *ContainerInstance, process *specs.Process) (*pb.ContainerSandboxExecResponse, error) {
	started := make(chan int, 1)

	processIO := NewSandboxProcessIO()

	var stdoutBuf, stderrBuf bytes.Buffer
	go func() {
		io.Copy(&stdoutBuf, processIO.Stdout())
	}()

	go func() {
		io.Copy(&stderrBuf, processIO.Stderr())
	}()

	if !in.Interactive {
		processIO.Stdin().Close()
	}

	go func() {
		err := s.containerRuntime.Exec(context.Background(), in.ContainerId, *process, &ContainerExecOpts{
			IO:      processIO,
			Started: started,
		})
		if err != nil {
			if exitErr, ok := err.(*runc.ExitError); ok {
				processIO.done <- exitErr.Status
			} else {
				processIO.done <- 1
			}
			return
		}

		processIO.done <- 0
	}()

	pid := <-started
	if pid == -1 {
		return &pb.ContainerSandboxExecResponse{
			Ok:  false,
			Pid: -1,
		}, nil
	}

	processState := &SandboxProcessState{
		Pid:       pid,
		Args:      process.Args,
		Stdin:     processIO.Stdin(),
		Stdout:    processIO.Stdout(),
		Stderr:    processIO.Stderr(),
		StartTime: time.Now(),
		Status:    SandboxProcessStatusRunning,
		StdoutBuf: &stdoutBuf,
		StderrBuf: &stderrBuf,
		mu:        sync.Mutex{},
		ExitCode:  -1,
	}
	instance.SandboxProcesses.Store(int32(pid), processState)

	go func() {
		exitCode := <-processIO.Done()

		if state, ok := instance.SandboxProcesses.Load(int32(pid)); ok {
			ps := state.(*SandboxProcessState)
			ps.ExitCode = exitCode
			ps.Status = SandboxProcessStatusExited
			ps.EndTime = time.Now()
			instance.SandboxProcesses.Store(int32(pid), ps)

			log.Info().Str("container_id", in.ContainerId).Int("pid", pid).Int("exit_code", exitCode).Msg("sandbox process exited")
		}
	}()

	return &pb.ContainerSandboxExecResponse{
		Ok:  true,
		Pid: int32(pid),
	}, nil
}

func (s *ContainerServer) ContainerSandboxStatus(ctx context.Context, in *pb.ContainerSandboxStatusRequest) (*pb.ContainerSandboxStatusResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxStatusResponse{
			Ok:       false,
			ErrorMsg: "Container not found",
		}, nil
	}

	processState, ok := instance.SandboxProcesses.Load(int32(in.Pid))
	if !ok {
		return &pb.ContainerSandboxStatusResponse{
			Ok:       false,
			ErrorMsg: "Process not found",
		}, nil
	}
	ps := processState.(*SandboxProcessState)

	return &pb.ContainerSandboxStatusResponse{
		Ok:       true,
		Status:   string(ps.Status),
		ExitCode: int32(ps.ExitCode),
	}, nil
}

func (s *ContainerServer) ContainerSandboxStdout(ctx context.Context, in *pb.ContainerSandboxStdoutRequest) (*pb.ContainerSandboxStdoutResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxStdoutResponse{
			Ok:       false,
			ErrorMsg: "Container not found",
		}, nil
	}

	processState, ok := instance.SandboxProcesses.Load(int32(in.Pid))
	if !ok {
		return &pb.ContainerSandboxStdoutResponse{
			Ok:       false,
			ErrorMsg: "Process not found",
		}, nil
	}

	ps := processState.(*SandboxProcessState)

	ps.mu.Lock()
	stdout := ps.StdoutBuf.String()
	ps.StdoutBuf.Reset()
	ps.mu.Unlock()

	return &pb.ContainerSandboxStdoutResponse{
		Ok:     true,
		Stdout: stdout,
	}, nil
}

func (s *ContainerServer) ContainerSandboxStderr(ctx context.Context, in *pb.ContainerSandboxStderrRequest) (*pb.ContainerSandboxStderrResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxStderrResponse{
			Ok:       false,
			ErrorMsg: "Container not found",
		}, nil
	}

	processState, ok := instance.SandboxProcesses.Load(int32(in.Pid))
	if !ok {
		return &pb.ContainerSandboxStderrResponse{
			Ok:       false,
			ErrorMsg: "Process not found",
		}, nil
	}

	ps := processState.(*SandboxProcessState)

	ps.mu.Lock()
	stderr := ps.StderrBuf.String()
	ps.StderrBuf.Reset()
	ps.mu.Unlock()

	return &pb.ContainerSandboxStderrResponse{
		Ok:     true,
		Stderr: stderr,
	}, nil
}

func (s *ContainerServer) ContainerSandboxKill(ctx context.Context, in *pb.ContainerSandboxKillRequest) (*pb.ContainerSandboxKillResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxKillResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	processState, ok := instance.SandboxProcesses.Load(int32(in.Pid))
	if !ok {
		return &pb.ContainerSandboxKillResponse{Ok: false, ErrorMsg: "Process not found"}, nil
	}

	ps := processState.(*SandboxProcessState)

	ps.mu.Lock()
	ps.Status = SandboxProcessStatusExited
	ps.EndTime = time.Now()
	ps.mu.Unlock()

	syscall.Kill(ps.Pid, syscall.SIGTERM)
	return &pb.ContainerSandboxKillResponse{Ok: true}, nil
}

func (s *ContainerServer) ContainerSandboxUploadFile(ctx context.Context, in *pb.ContainerSandboxUploadFileRequest) (*pb.ContainerSandboxUploadFileResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxUploadFileResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.ContainerSandboxUploadFileResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	err = os.WriteFile(filepath.Join(instance.Spec.Root.Path, filepath.Clean(containerPath)), in.Data, os.FileMode(in.Mode))
	if err != nil {
		return &pb.ContainerSandboxUploadFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.ContainerSandboxUploadFileResponse{Ok: true}, nil
}

func (s *ContainerServer) ContainerSandboxDownloadFile(ctx context.Context, in *pb.ContainerSandboxDownloadFileRequest) (*pb.ContainerSandboxDownloadFileResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxDownloadFileResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.ContainerSandboxDownloadFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	data, err := os.ReadFile(filepath.Join(instance.Spec.Root.Path, filepath.Clean(containerPath)))
	if err != nil {
		return &pb.ContainerSandboxDownloadFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.ContainerSandboxDownloadFileResponse{Ok: true, Data: data}, nil
}

func (s *ContainerServer) ContainerSandboxDeleteFile(ctx context.Context, in *pb.ContainerSandboxDeleteFileRequest) (*pb.ContainerSandboxDeleteFileResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxDeleteFileResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.ContainerSandboxDeleteFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	err = os.RemoveAll(filepath.Join(instance.Spec.Root.Path, filepath.Clean(containerPath)))
	if err != nil {
		return &pb.ContainerSandboxDeleteFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.ContainerSandboxDeleteFileResponse{Ok: true}, nil
}

func (s *ContainerServer) ContainerSandboxStatFile(ctx context.Context, in *pb.ContainerSandboxStatFileRequest) (*pb.ContainerSandboxStatFileResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxStatFileResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.ContainerSandboxStatFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	stat, err := os.Stat(filepath.Join(instance.Spec.Root.Path, filepath.Clean(containerPath)))
	if err != nil {
		return &pb.ContainerSandboxStatFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.ContainerSandboxStatFileResponse{Ok: true, FileInfo: &pb.FileInfo{
		Mode:        int32(stat.Mode()),
		Size:        stat.Size(),
		ModTime:     stat.ModTime().Unix(),
		Permissions: uint32(stat.Mode()),
		Owner:       strconv.Itoa(int(stat.Sys().(*syscall.Stat_t).Uid)),
		Group:       strconv.Itoa(int(stat.Sys().(*syscall.Stat_t).Gid)),
		IsDir:       stat.IsDir(),
		Name:        stat.Name(),
	}}, nil
}

func (s *ContainerServer) ContainerSandboxListFiles(ctx context.Context, in *pb.ContainerSandboxListFilesRequest) (*pb.ContainerSandboxListFilesResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxListFilesResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.ContainerSandboxListFilesResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	files, err := os.ReadDir(filepath.Join(instance.Spec.Root.Path, filepath.Clean(containerPath)))
	if err != nil {
		return &pb.ContainerSandboxListFilesResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	responseFiles := make([]*pb.FileInfo, 0)
	for _, file := range files {
		stat, err := file.Info()
		if err != nil {
			return &pb.ContainerSandboxListFilesResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}

		responseFiles = append(responseFiles, &pb.FileInfo{
			Mode:        int32(stat.Mode()),
			Size:        stat.Size(),
			ModTime:     stat.ModTime().Unix(),
			Permissions: uint32(stat.Mode()),
			Owner:       strconv.Itoa(int(stat.Sys().(*syscall.Stat_t).Uid)),
			Group:       strconv.Itoa(int(stat.Sys().(*syscall.Stat_t).Gid)),
			IsDir:       stat.IsDir(),
			Name:        file.Name(),
		})
	}

	return &pb.ContainerSandboxListFilesResponse{Ok: true, Files: responseFiles}, nil
}

func (s *ContainerServer) ContainerSandboxExposePort(ctx context.Context, in *pb.ContainerSandboxExposePortRequest) (*pb.ContainerSandboxExposePortResponse, error) {
	_, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxExposePortResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.ContainerSandboxExposePortResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	getAddressMapResponse, err := handleGRPCResponse(s.containerRepoClient.GetContainerAddressMap(context.Background(), &pb.GetContainerAddressMapRequest{
		ContainerId: in.ContainerId,
	}))
	if err != nil {
		return &pb.ContainerSandboxExposePortResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	addressMap := getAddressMapResponse.AddressMap
	if _, exists := addressMap[int32(in.Port)]; exists {
		return &pb.ContainerSandboxExposePortResponse{
			Ok:       false,
			ErrorMsg: fmt.Sprintf("Port %d is already exposed", in.Port),
		}, nil
	}

	bindPort, err := getRandomFreePort()
	if err != nil {
		return &pb.ContainerSandboxExposePortResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	err = s.containerNetworkManager.ExposePort(in.ContainerId, bindPort, int(in.Port))
	if err != nil {
		log.Error().Str("container_id", in.ContainerId).Msgf("failed to expose container bind port: %v", err)
		return &pb.ContainerSandboxExposePortResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	addressMap[int32(in.Port)] = fmt.Sprintf("%s:%d", s.podAddr, bindPort)
	setAddressMapResponse, err := handleGRPCResponse(s.containerRepoClient.SetContainerAddressMap(context.Background(), &pb.SetContainerAddressMapRequest{
		ContainerId: in.ContainerId,
		AddressMap:  addressMap,
	}))
	if err != nil {
		return &pb.ContainerSandboxExposePortResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	log.Info().Str("container_id", in.ContainerId).Msgf("exposed sandbox port %d to %s", in.Port, addressMap[int32(in.Port)])
	return &pb.ContainerSandboxExposePortResponse{Ok: setAddressMapResponse.Ok}, err
}
