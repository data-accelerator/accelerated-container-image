package builder

import (
	"context"
	"strings"
	"sync"

	"github.com/containerd/containerd/remotes"
	"github.com/containerd/containerd/remotes/docker"
	specs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type BuilderEngineType int

const (
	BuilderEngineTypeOverlayBD BuilderEngineType = iota
	BuilderEngineTypeFOCI
)

type Builder interface {
	Build(ctx context.Context) error
}

type BuilderOptions struct {
	Ref       string
	TargetRef string
	Auth      string
	PlainHTTP bool
	WorkDir   string
	Engine    BuilderEngineType
}

type overlaybdBuilder struct {
	layers int
	engine builderEngine
}

type builderEngine interface {
	downloadLayer(ctx context.Context, idx int) error

	// build layer archive, maybe tgz or zfile
	buildLayer(ctx context.Context, idx int) error

	uploadLayer(ctx context.Context, idx int) error

	// UploadImage upload new manifest and config
	uploadImage(ctx context.Context) error

	// cleanup remove workdir
	cleanup()
}

type builderEngineBase struct {
	fetcher  remotes.Fetcher
	pusher   remotes.Pusher
	manifest specs.Manifest
	config   specs.Image
	workDir  string
}

func NewOverlayBDBuilder(ctx context.Context, opt BuilderOptions) (Builder, error) {
	resolver := docker.NewResolver(docker.ResolverOptions{
		Credentials: func(s string) (string, string, error) {
			if opt.Auth == "" {
				return "", "", nil
			}
			authSplit := strings.Split(opt.Auth, ":")
			return authSplit[0], authSplit[1], nil
		},
		PlainHTTP: opt.PlainHTTP,
	})
	engineBase, err := getBuilderEngineBase(ctx, resolver, opt.Ref, opt.TargetRef)
	if err != nil {
		return nil, err
	}
	engineBase.workDir = opt.WorkDir
	var engine builderEngine
	switch opt.Engine {
	case BuilderEngineTypeOverlayBD:
		engine = NewOverlayBDBuilderEngine(engineBase)
	case BuilderEngineTypeFOCI:
		engine = NewFOCIBuilderEngine(engineBase)
	}
	return &overlaybdBuilder{
		layers: len(engineBase.manifest.Layers),
		engine: engine,
	}, nil
}

// TODO speed up with async
func (b *overlaybdBuilder) Build(ctx context.Context) error {
	defer b.engine.cleanup()
	downloaded := make([]chan error, b.layers)
	converted := make([]chan error, b.layers)
	var uploaded sync.WaitGroup

	errCh := make(chan error)
	defer close(errCh)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// collect error and kill all builder goroutines
	var retErr error
	retErr = nil
	go func() {
		select {
		case <-ctx.Done():
		case retErr = <-errCh:
		}
		if retErr != nil {
			cancel()
		}
	}()

	for i := 0; i < b.layers; i++ {
		downloaded[i] = make(chan error)
		converted[i] = make(chan error)

		// download goroutine
		go func(idx int) {
			defer close(downloaded[idx])
			if err := b.engine.downloadLayer(ctx, idx); err != nil {
				contextSendChan(ctx, errCh, errors.Wrapf(err, "failed to download layer %d", idx))
				return
			}
			logrus.Infof("downloaded layer %d", idx)
			contextSendChan(ctx, downloaded[idx], nil)
		}(i)

		// convert goroutine
		go func(idx int) {
			defer close(converted[idx])
			if contextReceiveChan(ctx, downloaded[idx]); ctx.Err() != nil {
				return
			}
			if idx > 0 {
				if contextReceiveChan(ctx, converted[idx-1]); ctx.Err() != nil {
					return
				}
			}
			if err := b.engine.buildLayer(ctx, idx); err != nil {
				contextSendChan(ctx, errCh, errors.Wrapf(err, "failed to convert layer %d", idx))
				return
			}
			logrus.Infof("layer %d converted", idx)
			// send to upload(idx) and convert(idx+1) once each
			contextSendChan(ctx, converted[idx], nil)
			if idx+1 < b.layers {
				contextSendChan(ctx, converted[idx], nil)
			}
		}(i)

		// upload goroutine
		uploaded.Add(1)
		go func(idx int) {
			defer uploaded.Done()
			if contextReceiveChan(ctx, converted[idx]); ctx.Err() != nil {
				return
			}
			if err := b.engine.uploadLayer(ctx, idx); err != nil {
				contextSendChan(ctx, errCh, errors.Wrapf(err, "failed to upload layer %d", idx))
				return
			}
			logrus.Infof("layer %d uploaded", idx)
		}(i)
	}
	uploaded.Wait()
	if retErr != nil {
		return retErr
	}

	if err := b.engine.uploadImage(ctx); err != nil {
		return errors.Wrap(err, "failed to upload manifest or config")
	}
	logrus.Info("convert finished")
	return nil
}

func getBuilderEngineBase(ctx context.Context, resolver remotes.Resolver, ref, targetRef string) (*builderEngineBase, error) {
	_, desc, err := resolver.Resolve(ctx, ref)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to resolve reference %q", ref)
	}
	fetcher, err := resolver.Fetcher(ctx, ref)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get fetcher for %q", ref)
	}
	pusher, err := resolver.Pusher(ctx, targetRef)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get pusher for %q", targetRef)
	}
	manifest, config, err := fetchManifestAndConfig(ctx, fetcher, desc)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch manifest and config")
	}
	return &builderEngineBase{
		fetcher:  fetcher,
		pusher:   pusher,
		manifest: manifest,
		config:   config,
	}, nil
}

// block until ctx.Done() or sended
func contextSendChan(ctx context.Context, ch chan<- error, value error) {
	select {
	case <-ctx.Done():
	case ch <- value:
	}
}

// block until ctx.Done() or received
func contextReceiveChan(ctx context.Context, ch <-chan error) {
	select {
	case <-ctx.Done():
	case <-ch:
	}
}
