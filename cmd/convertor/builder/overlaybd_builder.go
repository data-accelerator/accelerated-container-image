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
	"github.com/containerd/containerd/images"
	"github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

const (
	// labelKeyOverlayBDBlobDigest is the annotation key in the manifest to
	// describe the digest of blob in OverlayBD format.
	//
	// NOTE: The annotation is part of image layer blob's descriptor.
	labelKeyOverlayBDBlobDigest = "containerd.io/snapshot/overlaybd/blob-digest"

	// labelKeyOverlayBDBlobSize is the annotation key in the manifest to
	// describe the size of blob in OverlayBD format.
	//
	// NOTE: The annotation is part of image layer blob's descriptor.
	labelKeyOverlayBDBlobSize = "containerd.io/snapshot/overlaybd/blob-size"

	overlaybdBaseLayer = "/opt/overlaybd/baselayers/ext4_64"

	commitFile = "overlaybd.commit"
)

type overlaybdBuilderEngine struct {
	*builderEngineBase
	overlaybdConfig *snapshot.OverlayBDBSConfig
	overlaybdLayers []specs.Descriptor
}

func NewOverlayBDBuilderEngine(base *builderEngineBase) builderEngine {
	config := &snapshot.OverlayBDBSConfig{
		Lowers:     []snapshot.OverlayBDBSConfigLower{},
		ResultFile: "",
	}
	config.Lowers = append(config.Lowers, snapshot.OverlayBDBSConfigLower{
		File: overlaybdBaseLayer,
	})
	return &overlaybdBuilderEngine{
		builderEngineBase: base,
		overlaybdConfig:   config,
		overlaybdLayers:   make([]specs.Descriptor, len(base.manifest.Layers)),
	}
}

func (e *overlaybdBuilderEngine) downloadLayer(ctx context.Context, idx int) error {
	desc := e.manifest.Layers[idx]
	targetFile := path.Join(e.getLayerDir(idx), "layer.tar")
	return downloadLayer(ctx, e.fetcher, targetFile, desc, true)
}

func (e *overlaybdBuilderEngine) buildLayer(ctx context.Context, idx int) error {
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
	if err := overlaybdApply(ctx, layerDir); err != nil {
		return err
	}
	if err := overlaybdCommit(ctx, layerDir, commitFile); err != nil {
		return err
	}
	e.overlaybdConfig.Lowers = append(e.overlaybdConfig.Lowers, snapshot.OverlayBDBSConfigLower{
		File: path.Join(layerDir, commitFile),
	})
	os.Remove(path.Join(layerDir, "layer.tar"))
	os.Remove(path.Join(layerDir, "writable_data"))
	os.Remove(path.Join(layerDir, "writable_index"))
	return nil
}

func (e *overlaybdBuilderEngine) uploadLayer(ctx context.Context, idx int) error {
	layerDir := e.getLayerDir(idx)
	desc, err := getFileDesc(path.Join(layerDir, commitFile), false)
	if err != nil {
		return errors.Wrapf(err, "failed to get descriptor for layer %d", idx)
	}
	desc.MediaType = images.MediaTypeDockerSchema2Layer
	desc.Annotations = map[string]string{
		labelKeyOverlayBDBlobDigest: desc.Digest.String(),
		labelKeyOverlayBDBlobSize:   fmt.Sprintf("%d", desc.Size),
	}
	if err := uploadBlob(ctx, e.pusher, path.Join(layerDir, commitFile), desc); err != nil {
		return errors.Wrapf(err, "failed to upload layer %d", idx)
	}
	e.overlaybdLayers[idx] = desc
	return nil
}

func (e *overlaybdBuilderEngine) uploadImage(ctx context.Context) error {
	for idx := range e.manifest.Layers {
		e.manifest.Layers[idx] = e.overlaybdLayers[idx]
		e.config.RootFS.DiffIDs[idx] = e.overlaybdLayers[idx].Digest
	}
	baseDesc := specs.Descriptor{
		MediaType: images.MediaTypeDockerSchema2Layer,
		Digest:    "sha256:c3a417552a6cf9ffa959b541850bab7d7f08f4255425bf8b48c85f7b36b378d9",
		Size:      4737695,
		Annotations: map[string]string{
			labelKeyOverlayBDBlobDigest: "sha256:c3a417552a6cf9ffa959b541850bab7d7f08f4255425bf8b48c85f7b36b378d9",
			labelKeyOverlayBDBlobSize:   "4737695",
		},
	}
	if err := uploadBlob(ctx, e.pusher, overlaybdBaseLayer, baseDesc); err != nil {
		return errors.Wrapf(err, "failed to upload baselayer %q", overlaybdBaseLayer)
	}
	e.manifest.Layers = append([]specs.Descriptor{baseDesc}, e.manifest.Layers...)
	e.config.RootFS.DiffIDs = append([]digest.Digest{baseDesc.Digest}, e.config.RootFS.DiffIDs...)
	return uploadManifestAndConfig(ctx, e.pusher, e.manifest, e.config)
}

func (e *overlaybdBuilderEngine) cleanup() {
	os.RemoveAll(e.workDir)
}

func (e *overlaybdBuilderEngine) getLayerDir(idx int) string {
	return path.Join(e.workDir, fmt.Sprintf("%04d_", idx)+e.manifest.Layers[idx].Digest.String())
}

func overlaybdApply(ctx context.Context, dir string) error {
	binpath := filepath.Join("/opt/overlaybd/bin", "overlaybd-apply")

	out, err := exec.CommandContext(ctx, binpath,
		path.Join(dir, "layer.tar"),
		path.Join(dir, "config.json")).CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "failed to overlaybd-apply: %s", out)
	}
	return nil
}
