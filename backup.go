package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"cloud.google.com/go/storage"
	"github.com/BurntSushi/toml"
	"github.com/urfave/cli/v3"
)

//go:embed default-config.toml
var embeddedFiles embed.FS

func main() {
	cmd := &cli.Command{
		Name:   "create",
		Usage:  "create a backup of a directory",
		Action: createBackup,
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}

func createBackup(ctx context.Context, cmd *cli.Command) error {
	configPath, err := chooseConfigPath()
	if err != nil {
		log.Fatalf("choosing config path: %v", err)
	}
	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("loading config: %v", err)
	} else if cfg == nil {
		log.Printf("created missing config file: %s", configPath)
		log.Printf("please fill in, then re-run")
		return nil
	}

	return errors.New("unimplemented")
}

func connectToGCS(ctx context.Context) (*storage.Client, error) {
	client, err := storage.NewGRPCClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create grpc client: %w", err)
	}
	return client, nil
}

type Config struct {
	GCS GCSConfig `toml:"gcs"`
}

type GCSConfig struct {
	ProjectID string `toml:"project_id"`
	Bucket    string `toml:"bucket"`
	Prefix    string `toml:"prefix,omitempty"`
}

func chooseConfigPath() (string, error) {
	configDir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("failed to get user config dir: %w", err)
	}
	fp := filepath.Join(configDir, "lovely.software", "backup", "config.toml")
	return fp, nil
}

func loadConfig(path string) (*Config, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, fmt.Errorf("creating configuration dir: %w", err)
	}

	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			defaultConfig, err := embeddedFiles.ReadFile("default-config.toml")
			if err != nil {
				return nil, fmt.Errorf("failed to read default config: %w", err)
			}
			if err := os.WriteFile(path, defaultConfig, 0600); err != nil {
				return nil, fmt.Errorf("failed to write default config: %w", err)
			}
			return nil, nil
		}
		return nil, fmt.Errorf("failed to stat config file: %w", err)
	}

	cfg := new(Config)
	if _, err := toml.DecodeFile(path, cfg); err != nil {
		return nil, fmt.Errorf("decoding config file: %w", err)
	}

	return cfg, nil
}
