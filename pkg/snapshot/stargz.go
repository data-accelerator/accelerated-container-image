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

package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/reference"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/continuity"
	"github.com/docker/docker/pkg/mount"
	"github.com/sirupsen/logrus"
)

const (
	umountTimeOutSecs = 3.0                    //umount timeout secs
	umountSleepTime   = 200 * time.Millisecond //sleep time after each umount operation
)

//From CRI plugin
const (
	//CriImageRefLabel  is a label which contains image reference passed from CRI plugin.
	CriImageRefLabel = "containerd.io/snapshot/cri.image-ref"
	// CriLayerDigestLabel is a label which contains layer digest passed from CRI plugin.
	CriLayerDigestLabel = "containerd.io/snapshot/cri.layer-digest"
	// CriImageLayersLabel is a label which contains layer digests contained in the target image and is passed from CRI plugin.
	CriImageLayersLabel = "containerd.io/snapshot/cri.image-layers"
)

//From ctr
const (
	// targetRefLabel is a label which contains image reference.
	StargzRefLabel = "containerd.io/snapshot/remote/stargz.reference"

	// targetDigestLabel is a label which contains layer digest.
	StargzLayerDigestLabel = "containerd.io/snapshot/remote/stargz.digest"

	// targetImageLayersLabel is a label which contains layer digests contained in
	// the target image.
	StargzLayersLabel = "containerd.io/snapshot/remote/stargz.layers"
)

const (
	TOCJSONDigestAnnotation = "containerd.io/snapshot/stargz/toc.digest"

	remoteLabel    = "containerd.io/snapshot/remote"
	remoteLabelVal = "remote snapshot"
)

const (
	stargzDirName           = "astargz"
	layerInfoFile           = "layer_info.json"
	stargzBinary            = "/opt/overlaybd/bin/astargz"
	waitForMountSleepTime   = 100 * time.Millisecond
	waitForMountTimeOutSecs = 6
)

// StargzLayer is the config of each layer.
type StargzLayerConfig struct {
	BlobURL       string `json:"blob_url"`
	BlobDigest    string `json:"blob_digest"`
	BlobTocDigest string `json:"toc_digest"`
	ImageRef      string `json:"image_ref"`
	MountPoint    string `json:"mount_point"`
	StargzPath    string `json:"stargz_path"`
}

func (o *snapshotter) checkAndPrepareStargzForPullPhrase(ctx context.Context, key, target, id string, infoLabels map[string]string, opts ...snapshots.Opt) (bool, error) {
	logrus.Infof("Enter checkAndPrepareStargzForPullPhrase(), key:%s", key)
	defer logrus.Infof("Leave checkAndPrepareStargzForPullPhrase(), key:%s", key)

	prepareStargzMount := func(mountpoint string, labels map[string]string) (retErr error) {
		logrus.Infof("filesystem Mount(..), mountpoint:%s, labels:%+v", mountpoint, labels)
		if _, err := os.Stat(mountpoint); err != nil {
			logrus.Infof("mountpoint(%s) doesn't exist.", mountpoint)
		}
		if isMounted, err := mount.Mounted(mountpoint); err == nil {
			if isMounted {
				logrus.Infof("mountpoint has been mounted. mountpoint:%s", mountpoint)
				return nil
			}
		} else {
			logrus.Errorf("Failed to mount.Mounted(%s), err:%s", mountpoint, err)
			return err
		}

		//根据labels获取layer相应的地址信息，并save到相应的layer对应的layer_info.json文件中
		if err := constructAndSaveStargzLayerInfo(mountpoint, labels); err != nil {
			logrus.Errorf("Failed to constructAndSaveStargzLayerInfo(..),mountpoint:%s, labels:%+v", mountpoint, labels)
			return err
		}
		//以layer_info.json为参数，拉起fuse daemon，并mount到mountpoint上
		if err := launchFuseDaemonAndMount(mountpoint); err != nil {
			logrus.Errorf("Failed to launchFuseDaemonAndMount(%s)", mountpoint)
			return err
		}
		logrus.Infof("Success to Mount(..),mountpoint;%s", mountpoint)
		return nil
	}
	isStargzLayer := func() bool {
		_, ok1 := infoLabels[labelKeyTargetSnapshotRef]
		_, ok2 := infoLabels[TOCJSONDigestAnnotation]
		_, err1 := getLayerDigest(infoLabels)
		_, err2 := getImageRef(infoLabels)
		return ok1 && ok2 && (err1 == nil) && (err2 == nil)
	}

	if !stargzBinaryExists() {
		return false, nil
	}

	if !isStargzLayer() {
		logrus.Infof("No stargz image")
		return false, nil
	}

	if err := prepareStargzMount(o.upperPath(id), infoLabels); err != nil {
		logrus.Infof("failed to o.prepareRemoteSnapshot(...), key:%s, base.Labels:%+v, err:%s", key, infoLabels, err)
		return false, nil
	}
	infoLabels[remoteLabel] = remoteLabelVal // Mark this snapshot as remote
	_, _, err := o.commit(ctx, target, key, opts...)
	if err == nil || errdefs.IsAlreadyExists(err) {
		// count also AlreadyExists as "success"
		logrus.Infof("success to prepareStargzMount and return ErrAlreadyExists")
		return true, fmt.Errorf("target snapshot %q: %w", target, errdefs.ErrAlreadyExists)
		//return true, errors.Wrapf(errdefs.ErrAlreadyExists, "target snapshot %q", target)
	}
	logrus.Warnf("failed to parepare snapshotter, key:%s err:%s", key, err)
	return true, err
}

//lowers 是snDir不是fs dir
func (o *snapshotter) prepareStargzForRunPhrase(lowers []string) error {
	logrus.Infof("Enter prepareStargzForRunPhrase")
	defer logrus.Infof("Leave prepareStargzForRunPhrase")

	if !stargzBinaryExists() {
		return nil
	}

	for _, snDir := range lowers {
		is, _ := isStargzLayer(snDir)
		if !is {
			continue
		}

		fsdir := path.Join(snDir, "fs")
		if isMounted, err := mount.Mounted(fsdir); err == nil {
			if isMounted {
				logrus.Infof("mountpoint has been mounted. mountpoint:%s", fsdir)
				continue
			}
		} else {
			logrus.Errorf("Failed to mount.Mounted(%s), err:%s", fsdir, err)
			return err
		}
		if err := launchFuseDaemonAndMount(fsdir); err != nil {
			logrus.Errorf("Failed to stargz.LaunchFuseDaemonAndMount(%s)", fsdir)
		}
		//Need check mounted during run phrase.
		if err := waitForMounted(fsdir, waitForMountTimeOutSecs); err != nil {
			logrus.Errorf("Failed to waitforMounted(%s, %d), err:%s", fsdir, waitForMountTimeOutSecs, err)
			return err
		}
	}
	return nil
}

func (o *snapshotter) umountStargz(id string) error {
	umountMountPoint := func(mountPoint string) error {
		snDir := path.Dir(mountPoint)
		if b, _ := isStargzLayer(snDir); !b {
			return nil
		}

		if _, err := os.Stat(mountPoint); err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}

		logrus.Infof("enter umountMountPoint(mountPoint:%s)", mountPoint)
		start := time.Now()
		for {
			if err := mount.Unmount(mountPoint); err != nil {
				//Note we get mount state by mount.Mounted(.), so only record this error.
				logrus.Warnf("mount.Unmount(mountPoint_:%s), err:%s", mountPoint, err)
			}

			m, err := mount.Mounted(mountPoint)
			if err != nil {
				logrus.Errorf("leave umountMountPoint(mountPoint:%s), err:%s", mountPoint, err)
				return err
			}
			if !m {
				logrus.Infof("leave umountMountPoint(mountPoint:%s) success umount it. eclapse:%f secs",
					mountPoint, time.Since(start).Seconds())
				return nil
			}

			if time.Since(start).Seconds() > umountTimeOutSecs {
				logrus.Errorf("levave umountMountPoint, timeout(60secs) and can't umount %s", mountPoint)
				return fmt.Errorf("levave umountMountPoint, timeout(60secs) and can't umount %s", mountPoint)
			}
			time.Sleep(umountSleepTime)
		}
	}

	killStargzProcess := func(mountPoint string) error {
		searchStargzCmdLine := func() string {
			cmdLine := "ps -ef | grep -w " + stargzBinary
			cmdLine += " " + "| grep -w '\\-\\-layer_meta_path=" + layerInfoFilePath(mountPoint) + "'"
			cmdLine += " " + "| grep -v ' grep '"

			return cmdLine
		}
		getProcessNumByCmdline := func(cmdLine string) (int, error) {
			cmd := exec.Command("/bin/bash", "-c", cmdLine)
			out, err := cmd.Output()
			if err != nil {
				logrus.Errorf("ERROR failed to  cmd.Output(), cmdLine:%s, err:%s", cmdLine, err)
				return 0, err
			}

			strVal := strings.Trim(string(out), "\n ")
			num, err := strconv.Atoi(strVal)
			if err != nil {
				logrus.Errorf("ERROR Atoi(%s). cmdLine:%s, err:%s ", strVal, cmdLine, err)
				return 0, err
			}
			if num != 1 {
				logrus.Infof("getProcessNumByCmdline(...), get %d processes, cmdLine:%s", num, cmdLine)
			}
			return num, nil
		}
		getStargzProcessNum := func() (int, error) {
			cmdLine := searchStargzCmdLine()
			cmdLine += " " + "| wc -l"
			return getProcessNumByCmdline(cmdLine)
		}
		doKillStargz := func() error {
			cmdLine := searchStargzCmdLine()
			cmdLine += "|awk '{print $2}' | xargs kill -9"
			cmd := exec.Command("/bin/bash", "-c", cmdLine)
			out, err := cmd.Output()
			if err != nil {
				logrus.Errorf("Failed to doKillStargz(), cmdLine:%s, out:%s, err:%s", cmdLine, string(out), err)
			} else {
				logrus.Infof("Success to doKillStargz(), cmdLine:%s", cmdLine)
			}
			if num, err := getStargzProcessNum(); err != nil || num != 0 {
				errRet := fmt.Errorf("Failed to doKillStargz(). stargz process num is %d, err:%s", num, err)
				logrus.Errorf("%s", errRet)
				return errRet
			}
			logrus.Infof("Success to doKillStargz(), mountPoint:%s", mountPoint)
			return nil
		}
		stargzNum, err := getStargzProcessNum()
		if err != nil {
			logrus.Errorf("Failed to getStargzProcessNum(), err:%s", err)
			return err
		}
		if stargzNum == 0 {
			logrus.Infof("stargz process number is 0. So no need to kill astargz process. mountPoint:%s", mountPoint)
			return nil
		}
		return doKillStargz()
	}

	snDir := o.getSnDir(id)
	mp := path.Join(snDir, "fs")
	if err := umountMountPoint(mp); err != nil {
		logrus.Errorf("Failed to umountMountPoint(%s)", mp)
		return err
	}
	logrus.Infof("Success to umountMountPoint(%s)", mp)

	return killStargzProcess(mp)
}

func stargzBinaryExists() bool {
	if _, err := os.Stat(stargzBinary); err != nil {
		return false
	}
	return true
}

//launch fuse daemon
func launchFuseDaemonAndMount(mountpoint string) error {
	formCmd := func(layerMetaPath string) string {
		ret := fmt.Sprintf("%s --layer_meta_path=%s &", stargzBinary, layerMetaPath)
		return ret
	}

	layerMetaPath := layerInfoFilePath(mountpoint)
	cmdLine := formCmd(layerMetaPath)
	//将该layerMetaPath传递给fuse daemon
	cmd := exec.Command("/bin/bash", "-c", cmdLine)
	err := cmd.Run()
	if err != nil {
		logrus.Errorf("Failed to exec command:%s, err:%s", cmdLine, err)
		return err
	}
	logrus.Infof("Succ to exec command:%s", cmdLine)
	return nil
}

func waitForMounted(mp string, timeoutSecs float64) error {
	start := time.Now()
	for {
		m, err := mount.Mounted(mp)
		if err != nil {
			logrus.Errorf("Failed to mount.Mounted(%s), err:%s", mp, err)
			return err
		}
		if m {
			return nil
		}
		time.Sleep(waitForMountSleepTime)
		if time.Since(start).Seconds() > timeoutSecs {
			return fmt.Errorf("waitforMounted timeout. timeoutSecs:%f", timeoutSecs)
		}
	}
}

func constructAndSaveStargzLayerInfo(mountpoint string, labels map[string]string) error {
	checkLayerConfig := func(cfg *StargzLayerConfig) error {
		if cfg.BlobURL == "" {
			return fmt.Errorf("blobURL is empty")
		}
		if cfg.BlobDigest == "" {
			return fmt.Errorf("blobDigest is empty")
		}
		/*
			if cfg.BlobTocDigest == "" {
				return fmt.Errorf("BlobTocDigest is empty.")
			}*/
		if cfg.ImageRef == "" {
			logrus.Warnf("ImageRef is empty.")
		}
		return nil
	}

	infoFilePath := layerInfoFilePath(mountpoint)
	if _, err := os.Stat(infoFilePath); err == nil {
		return nil
	}

	urlPrefix, err := getLayerBlobURLPrefix(labels)
	if err != nil {
		logrus.Errorf("Failed to getLayerBlobURLPrefix(labels), err:%s", err)
		return err
	}

	layerDigest, err := getLayerDigest(labels)
	if err != nil {
		return err
	}

	imageRef, err := getImageRef(labels)
	if err != nil {
		return err
	}

	dstDir := getStargzDir(mountpoint)
	info := &StargzLayerConfig{
		BlobURL:       fmt.Sprintf("%s/%s", urlPrefix, layerDigest),
		BlobDigest:    layerDigest,
		BlobTocDigest: labels[TOCJSONDigestAnnotation],
		ImageRef:      imageRef,
		MountPoint:    mountpoint,
		StargzPath:    dstDir,
	}

	if err := checkLayerConfig(info); err != nil {
		logrus.Errorf("Failed to checkLayerConfig(info), err:%s", err)
		return err
	}

	//转换成json文件并保存下来
	if err := os.MkdirAll(dstDir, 0755); err != nil && os.IsNotExist(err) {
		logrus.Errorf("Failed to os.MkdirAll(%s, 0755), err:%s", dstDir, err)
		return err
	}

	data, err := json.Marshal(info)
	if err != nil {
		logrus.Errorf("Failed to json.Marshal(info), err:%s", err)
		return fmt.Errorf("Failed to json.Marshal(info), err:%s", err)
	}

	if err := continuity.AtomicWriteFile(infoFilePath, data, 0600); err != nil {
		return fmt.Errorf("Failed to continuity.AtomicWriteFile(..), infoFilePath:%s", infoFilePath)
	}
	return nil
}

func getImageRef(labels map[string]string) (string, error) {
	keys := []string{CriImageRefLabel, labelKeyImageRef}
	if ret, ok := getValueFromLabels(labels, keys); ok {
		return ret, nil
	}

	logrus.Errorf("Can't get image ref. labels:%+v, keys:%+v", labels, keys)
	return "", fmt.Errorf("can't get image ref")
}

func getLayerDigest(labels map[string]string) (string, error) {
	keys := []string{CriLayerDigestLabel, StargzLayerDigestLabel}
	if ret, ok := getValueFromLabels(labels, keys); ok {
		return ret, nil
	}

	logrus.Errorf("Can't get layer digest. labels:%+v, keys:%+v", labels, keys)
	return "", fmt.Errorf("can't get get layer digest")
}

func getValueFromLabels(labels map[string]string, keys []string) (string, bool) {
	for _, key := range keys {
		if val, ok := labels[key]; ok && val != "" {
			return val, true
		}
	}
	return "", false
}

func getLayerBlobURLPrefix(labels map[string]string) (string, error) {
	constructImageBlobURL := func(ref string) (string, error) {
		refspec, err := reference.Parse(ref)
		if err != nil {
			logrus.Errorf("Failed to reference.Parse(%s), err:%s", ref, err)
			return "", fmt.Errorf("Failed to reference.Parse(%s), err:%s", ref, err)
		}

		host := refspec.Hostname()
		repo := strings.TrimPrefix(refspec.Locator, host+"/")
		return "https://" + path.Join(host, "v2", repo) + "/blobs", nil
	}
	ref, err := getImageRef(labels)
	if err != nil {
		return "", err
	}
	return constructImageBlobURL(ref)
}

func layerInfoFilePath(mountpoint string) string {
	return path.Join(getStargzDir(mountpoint), layerInfoFile)
}

func getStargzDir(mountpoint string) string {
	ret := path.Join(path.Dir(mountpoint), stargzDirName)
	return ret
}

func isStargzLayer(snDir string) (bool, error) {
	stargzDir := path.Join(snDir, stargzDirName)
	return pathExists(stargzDir)
}
