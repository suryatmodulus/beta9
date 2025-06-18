package worker

import (
	"context"
	"io"
	"time"

	runc "github.com/beam-cloud/go-runc"
	"github.com/opencontainers/runtime-spec/specs-go"
)

type ContainerRuntime interface {
	Events(context context.Context, id string, interval time.Duration) (chan *ContainerEvent, error)
	Create(context context.Context, id string, bundle string, opts *ContainerCreateOpts) error
	Start(context context.Context, id string) error
	Delete(context context.Context, id string, opts *ContainerDeleteOpts) error
	Kill(context context.Context, id string, sig int, opts *ContainerKillOpts) error
	Stats(context context.Context, id string) (*Stats, error)
	State(context context.Context, id string) (*Container, error)
	Exec(context context.Context, id string, spec specs.Process, opts *ContainerExecOpts) error
	Run(context context.Context, id, bundle string, opts *ContainerCreateOpts) (int, error)
	Checkpoint(context context.Context, id string, opts *ContainerCheckpointOpts, actions ...runc.CheckpointAction) error
	Restore(context context.Context, id, bundle string, opts *ContainerRestoreOpts) (int, error)
}

func NewContainerRuntime(runc runc.Runc) ContainerRuntime {
	return &RuncContainerRuntime{runc: runc}
}

// ContainerCreateOpts wraps runc.CreateOpts
type ContainerCreateOpts struct {
	OutputWriter io.Writer
	Started      chan int
}

// ContainerDeleteOpts wraps runc.DeleteOpts
type ContainerDeleteOpts struct {
	Force bool
}

// ContainerKillOpts wraps runc.KillOpts
type ContainerKillOpts struct {
	All bool
}

// ContainerExecOpts wraps runc.ExecOpts
type ContainerExecOpts struct {
	OutputWriter io.Writer
	IO           runc.IO
	Started      chan int
}

// ContainerCheckpointOpts wraps runc.CheckpointOpts
type ContainerCheckpointOpts struct {
	LeaveRunning bool
	SkipInFlight bool
	AllowOpenTCP bool
	LinkRemap    bool
	ImagePath    string
	WorkDir      string
	OutputWriter io.Writer
}

// ContainerRestoreOpts wraps runc.RestoreOpts
type ContainerRestoreOpts struct {
	CheckpointOpts ContainerCheckpointOpts
	Started        chan int
}

// Container wraps runc.Container
type Container struct {
	*runc.Container
}

// Stats wraps runc.Stats
type Stats struct {
	*runc.Stats
}

// ContainerEvent wraps runc.Event
type ContainerEvent struct {
	*runc.Event
}

// Re-export the types from go-runc to avoid linter errors
type Cpu = runc.Cpu
type Memory = runc.Memory
type Pids = runc.Pids
type Blkio = runc.Blkio
type Hugetlb = runc.Hugetlb
type NetworkInterface = runc.NetworkInterface

// RuncContainerRuntime implements ContainerRuntime using runc.Runc
type RuncContainerRuntime struct {
	runc runc.Runc
}

// NewRuncContainerRuntime creates a new RuncContainerRuntime
func NewRuncContainerRuntime(runc runc.Runc) *RuncContainerRuntime {
	return &RuncContainerRuntime{runc: runc}
}

func (r *RuncContainerRuntime) Events(ctx context.Context, id string, interval time.Duration) (chan *ContainerEvent, error) {
	events, err := r.runc.Events(ctx, id, interval)
	if err != nil {
		return nil, err
	}

	containerEvents := make(chan *ContainerEvent, cap(events))
	go func() {
		defer close(containerEvents)
		for event := range events {
			containerEvents <- &ContainerEvent{Event: event}
		}
	}()

	return containerEvents, nil
}

func (r *RuncContainerRuntime) Create(ctx context.Context, id string, bundle string, opts *ContainerCreateOpts) error {
	var runcOpts *runc.CreateOpts
	if opts != nil {
		runcOpts = &runc.CreateOpts{
			OutputWriter: opts.OutputWriter,
			Started:      opts.Started,
		}
	}
	return r.runc.Create(ctx, id, bundle, runcOpts)
}

func (r *RuncContainerRuntime) Start(ctx context.Context, id string) error {
	return r.runc.Start(ctx, id)
}

func (r *RuncContainerRuntime) Delete(ctx context.Context, id string, opts *ContainerDeleteOpts) error {
	var runcOpts *runc.DeleteOpts
	if opts != nil {
		runcOpts = &runc.DeleteOpts{
			Force: opts.Force,
		}
	}
	return r.runc.Delete(ctx, id, runcOpts)
}

func (r *RuncContainerRuntime) Kill(ctx context.Context, id string, sig int, opts *ContainerKillOpts) error {
	var runcOpts *runc.KillOpts
	if opts != nil {
		runcOpts = &runc.KillOpts{
			All: opts.All,
		}
	}
	return r.runc.Kill(ctx, id, sig, runcOpts)
}

func (r *RuncContainerRuntime) Stats(ctx context.Context, id string) (*Stats, error) {
	stats, err := r.runc.Stats(ctx, id)
	if err != nil {
		return nil, err
	}
	return &Stats{Stats: stats}, nil
}

func (r *RuncContainerRuntime) State(ctx context.Context, id string) (*Container, error) {
	container, err := r.runc.State(ctx, id)
	if err != nil {
		return nil, err
	}
	return &Container{Container: container}, nil
}

func (r *RuncContainerRuntime) Exec(ctx context.Context, id string, spec specs.Process, opts *ContainerExecOpts) error {
	var runcOpts *runc.ExecOpts
	if opts != nil {
		runcOpts = &runc.ExecOpts{
			OutputWriter: opts.OutputWriter,
			IO:           opts.IO,
			Started:      opts.Started,
		}
	}
	return r.runc.Exec(ctx, id, spec, runcOpts)
}

func (r *RuncContainerRuntime) Run(ctx context.Context, id, bundle string, opts *ContainerCreateOpts) (int, error) {
	var runcOpts *runc.CreateOpts
	if opts != nil {
		runcOpts = &runc.CreateOpts{
			OutputWriter: opts.OutputWriter,
			Started:      opts.Started,
		}
	}
	return r.runc.Run(ctx, id, bundle, runcOpts)
}

func (r *RuncContainerRuntime) Checkpoint(ctx context.Context, id string, opts *ContainerCheckpointOpts, actions ...runc.CheckpointAction) error {
	var runcOpts *runc.CheckpointOpts
	if opts != nil {
		runcOpts = &runc.CheckpointOpts{
			LeaveRunning: opts.LeaveRunning,
			SkipInFlight: opts.SkipInFlight,
			AllowOpenTCP: opts.AllowOpenTCP,
			LinkRemap:    opts.LinkRemap,
			ImagePath:    opts.ImagePath,
			WorkDir:      opts.WorkDir,
			OutputWriter: opts.OutputWriter,
		}
	}
	return r.runc.Checkpoint(ctx, id, runcOpts, actions...)
}

func (r *RuncContainerRuntime) Restore(ctx context.Context, id, bundle string, opts *ContainerRestoreOpts) (int, error) {
	var runcOpts *runc.RestoreOpts
	if opts != nil {
		runcOpts = &runc.RestoreOpts{
			CheckpointOpts: runc.CheckpointOpts{
				LeaveRunning: opts.CheckpointOpts.LeaveRunning,
				SkipInFlight: opts.CheckpointOpts.SkipInFlight,
				AllowOpenTCP: opts.CheckpointOpts.AllowOpenTCP,
				LinkRemap:    opts.CheckpointOpts.LinkRemap,
				ImagePath:    opts.CheckpointOpts.ImagePath,
				WorkDir:      opts.CheckpointOpts.WorkDir,
				OutputWriter: opts.CheckpointOpts.OutputWriter,
			},
			Started: opts.Started,
		}
	}
	return r.runc.Restore(ctx, id, bundle, runcOpts)
}
