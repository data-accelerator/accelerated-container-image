/*
   Copyright The Accelerated Container Image Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package builder

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"

	"github.com/containerd/accelerated-container-image/pkg/snapshot"
	"github.com/containerd/containerd/archive/compression"
	"github.com/containerd/containerd/images"
	"github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

const (
	// labelKeyFOCI contains OCI image layer digests witch the foci index points
	labelKeyFOCI = "containerd.io/snapshot/overlaybd/fastoci"

	// TODO make foci baselayer compatible with overlaybd
	fociBaseLayer = "/opt/overlaybd/baselayers/foci-base"

	// index of OCI layers (gzip)
	gzipMetaFile = "gzip.meta"

	// index of block device
	fsMetaFile = "ext4.fs.meta"

	// foci index layer (gzip)
	fociLayerTar = "foci.tar.gz"

	// fociIdentifier is an empty file just used as a identifier
	fociIdentifier = ".fastoci.overlaybd"
)

type fastOCIBuilderEngine struct {
	*builderEngineBase
	overlaybdConfig *snapshot.OverlayBDBSConfig
	fociLayers      []specs.Descriptor
}

func NewFastOCIBuilderEngine(base *builderEngineBase) builderEngine {
	config := &snapshot.OverlayBDBSConfig{
		Lowers:     []snapshot.OverlayBDBSConfigLower{},
		ResultFile: "",
	}
	config.Lowers = append(config.Lowers, snapshot.OverlayBDBSConfigLower{
		File: fociBaseLayer,
	})
	return &fastOCIBuilderEngine{
		builderEngineBase: base,
		overlaybdConfig:   config,
		fociLayers:        make([]specs.Descriptor, len(base.manifest.Layers)),
	}
}

func (e *fastOCIBuilderEngine) downloadLayer(ctx context.Context, idx int) error {
	desc := e.manifest.Layers[idx]
	targetFile := path.Join(e.getLayerDir(idx), "layer.tar")
	return downloadLayer(ctx, e.fetcher, targetFile, desc, false)
}

func (e *fastOCIBuilderEngine) buildLayer(ctx context.Context, idx int) error {
	layerDir := e.getLayerDir(idx)
	if err := prepareWritableLayer(ctx, layerDir); err != nil {
		return err
	}
	e.overlaybdConfig.Upper = snapshot.OverlayBDBSConfigUpper{
		Data:  path.Join(layerDir, "writable_data"),
		Index: path.Join(layerDir, "writable_index"),
	}
	if err := writeConfig(layerDir, e.overlaybdConfig); err != nil {
		return err
	}
	if err := fociApply(ctx, layerDir); err != nil {
		return err
	}
	if err := overlaybdCommit(ctx, layerDir, fsMetaFile); err != nil {
		return err
	}
	// For now, overlaybd-foci-apply generate gzip.meta at workdir
	// TODO refactor this
	if err := os.Rename(gzipMetaFile, path.Join(layerDir, gzipMetaFile)); err != nil {
		return errors.Wrapf(err, "failed to move file %q", gzipMetaFile)
	}
	if err := e.createIdentifier(idx); err != nil {
		return errors.Wrapf(err, "failed to create identifier %q", fociIdentifier)
	}
	if err := buildArchiveFromFiles(ctx, path.Join(layerDir, fociLayerTar), compression.Gzip,
		path.Join(layerDir, fsMetaFile),
		path.Join(layerDir, gzipMetaFile),
		path.Join(layerDir, fociIdentifier),
	); err != nil {
		return errors.Wrapf(err, "failed to create foci archive for layer %d", idx)
	}
	e.overlaybdConfig.Lowers = append(e.overlaybdConfig.Lowers, snapshot.OverlayBDBSConfigLower{
		DataFile: path.Join(layerDir, "layer.tar"),
		File:     path.Join(layerDir, fsMetaFile),
	})
	os.Remove(path.Join(layerDir, "writable_data"))
	os.Remove(path.Join(layerDir, "writable_index"))
	return nil
}

func (e *fastOCIBuilderEngine) uploadLayer(ctx context.Context, idx int) error {
	layerDir := e.getLayerDir(idx)
	desc, err := getFileDesc(path.Join(layerDir, fociLayerTar), false)
	if err != nil {
		return errors.Wrapf(err, "failed to get descriptor for layer %d", idx)
	}
	desc.MediaType = specs.MediaTypeImageLayerGzip
	desc.Annotations = map[string]string{
		labelKeyOverlayBDBlobDigest: desc.Digest.String(),
		labelKeyOverlayBDBlobSize:   fmt.Sprintf("%d", desc.Size),
		labelKeyFOCI:                e.manifest.Layers[idx].Digest.String(),
	}
	if err := uploadBlob(ctx, e.pusher, path.Join(layerDir, fociLayerTar), desc); err != nil {
		return errors.Wrapf(err, "failed to upload layer %d", idx)
	}
	e.fociLayers[idx] = desc
	return nil
}

func (e *fastOCIBuilderEngine) uploadImage(ctx context.Context) error {
	for idx := range e.manifest.Layers {
		layerDir := e.getLayerDir(idx)
		uncompress, err := getFileDesc(path.Join(layerDir, fociLayerTar), true)
		if err != nil {
			return errors.Wrapf(err, "failed to get uncompressed descriptor for layer %d", idx)
		}
		e.manifest.Layers[idx] = e.fociLayers[idx]
		e.config.RootFS.DiffIDs[idx] = uncompress.Digest
	}
	// TODO make foci baselayer compatible with overlaybd
	baseDesc := specs.Descriptor{
		MediaType: images.MediaTypeDockerSchema2Layer,
		Digest:    "sha256:936469fc5a3adbac0ae7017a7a5e3fd013950b4d8c1f7acf2924cb7e8cdf3c8d",
		Size:      784349,
		Annotations: map[string]string{
			labelKeyOverlayBDBlobDigest: "sha256:936469fc5a3adbac0ae7017a7a5e3fd013950b4d8c1f7acf2924cb7e8cdf3c8d",
			labelKeyOverlayBDBlobSize:   "784349",
		},
	}
	if err := uploadBlob(ctx, e.pusher, fociBaseLayer, baseDesc); err != nil {
		return errors.Wrapf(err, "failed to upload baselayer %q", fociBaseLayer)
	}
	e.manifest.Layers = append([]specs.Descriptor{baseDesc}, e.manifest.Layers...)
	e.config.RootFS.DiffIDs = append([]digest.Digest{baseDesc.Digest}, e.config.RootFS.DiffIDs...)
	return uploadManifestAndConfig(ctx, e.pusher, e.manifest, e.config)
}

func (e fastOCIBuilderEngine) cleanup() {
	os.RemoveAll(e.workDir)
}

func (e *fastOCIBuilderEngine) getLayerDir(idx int) string {
	return path.Join(e.workDir, fmt.Sprintf("%04d_", idx)+e.manifest.Layers[idx].Digest.String())
}

func (e *fastOCIBuilderEngine) createIdentifier(idx int) error {
	targetFile := path.Join(e.getLayerDir(idx), fociIdentifier)
	file, err := os.Create(targetFile)
	if err != nil {
		return errors.Wrapf(err, "failed to create identifier file %q", fociIdentifier)
	}
	defer file.Close()
	return nil
}

func fociApply(ctx context.Context, dir string) error {
	binpath := filepath.Join("/opt/overlaybd/bin", "overlaybd-foci-apply")

	out, err := exec.CommandContext(ctx, binpath,
		path.Join(dir, "layer.tar"),
		path.Join(dir, "config.json")).CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "failed to overlaybd-foci-apply: %s", out)
	}
	return nil
}
