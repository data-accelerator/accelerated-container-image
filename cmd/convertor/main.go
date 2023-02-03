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

package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/containerd/accelerated-container-image/cmd/convertor/builder"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	repo      string
	user      string
	plain     bool
	tagInput  string
	tagOutput string
	dir       string
	foci      string
	overlaybd string

	rootCmd = &cobra.Command{
		Use:   "overlaybd-convertor",
		Short: "An image conversion tool from oci image to overlaybd image.",
		Long:  "overlaybd-convertor is a standalone userspace image conversion tool that helps converting oci images to overlaybd images",
		Run: func(cmd *cobra.Command, args []string) {
			if overlaybd == "" && foci == "" {
				if tagOutput == "" {
					logrus.Error("output-tag is required, you can specify it by [-o|--overlaybd|--foci]")
					os.Exit(1)
				}
				overlaybd = tagOutput
			}

			ctx := context.Background()
			opt := builder.BuilderOptions{
				Ref:       repo + ":" + tagInput,
				Auth:      user,
				PlainHTTP: plain,
				WorkDir:   dir,
			}
			if overlaybd != "" {
				logrus.Info("building overlaybd ...")
				opt.Engine = builder.BuilderEngineTypeOverlayBD
				opt.TargetRef = repo + ":" + overlaybd
				builder, err := builder.NewOverlayBDBuilder(ctx, opt)
				if err != nil {
					logrus.Errorf("failed to create overlaybd builder: %v", err)
					os.Exit(1)
				}
				if err := builder.Build(ctx); err != nil {
					logrus.Errorf("failed to build overlaybd: %v", err)
					os.Exit(1)
				}
				logrus.Info("overlaybd build finished")
			}
			if foci != "" {
				logrus.Info("building foci ...")
				opt.Engine = builder.BuilderEngineTypeFOCI
				opt.TargetRef = repo + ":" + foci
				builder, err := builder.NewOverlayBDBuilder(ctx, opt)
				if err != nil {
					logrus.Errorf("failed to create foci builder: %v", err)
					os.Exit(1)
				}
				if err := builder.Build(ctx); err != nil {
					logrus.Errorf("failed to build foci: %v", err)
					os.Exit(1)
				}
				logrus.Info("foci build finished")
			}
		},
	}
)

func init() {
	rootCmd.Flags().SortFlags = false
	rootCmd.Flags().StringVarP(&repo, "repository", "r", "", "repository for converting image (required)")
	rootCmd.Flags().StringVarP(&user, "username", "u", "", "user[:password] Registry user and password")
	rootCmd.Flags().BoolVarP(&plain, "plain", "", false, "connections using plain HTTP")
	rootCmd.Flags().StringVarP(&tagInput, "input-tag", "i", "", "tag for image converting from (required)")
	rootCmd.Flags().StringVarP(&tagOutput, "output-tag", "o", "", "tag for image converting to (required)")
	rootCmd.Flags().StringVarP(&dir, "dir", "d", "tmp_conv", "directory used for temporary data")
	rootCmd.Flags().StringVar(&foci, "foci", "", "build foci format")
	rootCmd.Flags().StringVar(&overlaybd, "overlaybd", "", "build overlaybd format")

	rootCmd.MarkFlagRequired("repository")
	rootCmd.MarkFlagRequired("input-tag")
}

func main() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	go func() {
		<-sigChan
		os.Exit(0)
	}()

	rootCmd.Execute()
}
