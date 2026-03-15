package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/BurntSushi/toml"
	"github.com/restic/chunker"
	"github.com/urfave/cli/v3"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
)

//go:embed default-config.toml
var embeddedFiles embed.FS

func main() {
	cmd := &cli.Command{
		Commands: []*cli.Command{
			{

				Name:   "create",
				Usage:  "create a backup of one or more directories",
				Action: createBackup,
				Flags: []cli.Flag{
					&cli.IntFlag{
						Name:  "cpus",
						Usage: "number of cpus to use",
						Value: max(runtime.NumCPU()-1, 1),
					},
					&cli.BoolFlag{
						Name:  "ignore-hidden",
						Usage: "whether to ignore hidden files or not",
						Value: true,
					},
				},
				Arguments: []cli.Argument{
					&cli.StringArgs{
						Name: "dirs",
						Max:  -1,
					},
				},
			},
		},
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

	dirs := cmd.StringArgs("dirs")
	if len(dirs) == 0 {
		log.Printf("please provide at least one directory to backup")
		return nil
	}

	cpus := cmd.Int("cpus")
	if cpus == 0 {
		return errors.New("--cpus must be non-zero")
	}
	ignoreHidden := cmd.Bool("ignore-hidden")

	storageClient, err := connectToGCS(ctx)
	if err != nil {
		return fmt.Errorf("connecting to gcs: %w", err)
	}
	defer storageClient.Close()

	start := time.Now()
	packIndexes := loadPackIndexes(ctx, cfg, storageClient, cpus)
	existingBlobs, err := buildExistingBlobSet(packIndexes)
	if err != nil {
		return fmt.Errorf("failed to build existing blob set: %w", err)
	}
	log.Printf(
		"built existing blob set in %d ms, found %d blobs",
		time.Since(start).Milliseconds(),
		len(existingBlobs),
	)

	eg := new(errgroup.Group)
	eg.SetLimit(cpus)

	for _, dir := range dirs {
		abs, err := filepath.Abs(dir)
		if err != nil {
			return fmt.Errorf("constructing absolute path for dir %s: %w", err)
		}

		if err := filepath.WalkDir(abs, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			// TODO this doesn't work on windows, see:
			// https://stackoverflow.com/questions/70291933/how-to-detect-hidden-files-in-a-folder-in-go-cross-platform-approach
			shouldSkip := ignoreHidden && strings.HasPrefix(filepath.Base(path), ".")
			if d.IsDir() {
				if shouldSkip {
					return fs.SkipDir
				} else {
					return nil
				}
			} else if shouldSkip {
				return nil
			}

			eg.Go(func() error {
				f, err := os.Open(path)
				if err != nil {
					return fmt.Errorf("opening file at path %s: %w", path, err)
				}
				defer f.Close()

				var numChunks int
				for chk, err := range chunkFile(f) {
					if err != nil {
						return fmt.Errorf("chunking file %s: %w", path, err)
					}
					_ = chk
					numChunks += 1
				}

				log.Printf("walking %s, found %d chunks\n", path, numChunks)
				return nil
			})
			return nil
		}); err != nil {
			return fmt.Errorf("walking dir %s: %w", dir, err)
		}
	}

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("chunking files: %w", err)
	}

	return nil
}

func connectToGCS(ctx context.Context) (*storage.Client, error) {
	client, err := storage.NewGRPCClient(ctx, storage.WithDisabledClientMetrics())
	if err != nil {
		return nil, fmt.Errorf("failed to create grpc client: %w", err)
	}
	return client, nil
}

type Config struct {
	GCS GCSConfig `toml:"gcs"`
}

type GCSConfig struct {
	Bucket string `toml:"bucket"`
	Prefix string `toml:"prefix,omitempty"`
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

	cfg.GCS.Prefix = strings.TrimSuffix(cfg.GCS.Prefix, "/")

	return cfg, nil
}

// TODO maybe use fastcdc or something if throughput becomes a bottleneck
// here, e.g. restic/chunker uses rabin fingerprinting. don't expect this
// to be an issue in most cases, only for large filesystems
func chunkFile(rdr io.Reader) iter.Seq2[[]byte, error] {
	const (
		minChunkSize uint = 512 << 10
		maxChunkSize uint = 8 << 20
		pol               = chunker.Pol(16194416975600741) // computed with chunker.RandomPolynomial
	)
	var (
		chunkerInst = chunker.NewWithBoundaries(rdr, pol, minChunkSize, maxChunkSize)
		data        = make([]byte, maxChunkSize)
	)
	return func(yield func([]byte, error) bool) {
		for {
			next, err := chunkerInst.Next(data)
			if err == io.EOF {
				return
			} else if err != nil {
				_ = yield(nil, fmt.Errorf("computing file chunk: %w", err))
				return
			}
			if !yield(next.Data, nil) {
				return
			}
		}
	}
}

type packIndexEntry struct {
	Offset uint `json:"offset"`
	Length uint `json:"length"`
}

type packIndex map[string]packIndexEntry

type keyedPackIndex struct {
	key   string
	index packIndex
}

// list and download files matching <prefix>/indexes/<pack-id>.json,
// which stores which blobs are in what pack file, and where in the file.
func loadPackIndexes(
	ctx context.Context,
	cfg *Config,
	client *storage.Client,
	concurrency int,
) iter.Seq2[keyedPackIndex, error] {
	var (
		prefix = fmt.Sprintf("%s/indexes/", cfg.GCS.Prefix)
		bucket = client.Bucket(cfg.GCS.Bucket)
		it     = bucket.Objects(ctx, &storage.Query{
			Prefix:    prefix,
			Delimiter: "/",
		})
	)

	type packIndexOrErr struct {
		kpi keyedPackIndex
		err error
	}

	load := func(f string) packIndexOrErr {
		rdr, err := bucket.Object(f).NewReader(ctx)
		if err != nil {
			return packIndexOrErr{err: fmt.Errorf("reading %s: %w", f, err)}
		}
		defer rdr.Close()
		var idxObject packIndex
		if err := json.NewDecoder(rdr).Decode(&idxObject); err != nil {
			return packIndexOrErr{err: fmt.Errorf("unmarshaling %s: %w", f, err)}
		}
		return packIndexOrErr{
			kpi: keyedPackIndex{
				key:   strings.TrimSuffix(path.Base(f), ".json"),
				index: idxObject,
			},
		}
	}

	c := make(chan packIndexOrErr)

	go func() {
		eg := new(errgroup.Group)
		eg.SetLimit(concurrency)
		for {
			attrs, err := it.Next()
			if err != nil {
				if err != iterator.Done {
					c <- packIndexOrErr{
						err: fmt.Errorf("listing pack indexes at prefix %s: %w", prefix, err),
					}
				}
				break
			}
			eg.Go(func() error {
				c <- load(attrs.Name)
				return nil
			})
		}
		_ = eg.Wait()
		close(c)
	}()

	return func(yield func(keyedPackIndex, error) bool) {
		for res := range c {
			if !yield(res.kpi, res.err) {
				return
			}
		}
	}
}

// Using existing pack indicies, construct a set of all blobs we've seen so far.
func buildExistingBlobSet(
	packIndicies iter.Seq2[keyedPackIndex, error],
) (map[string]struct{}, error) {
	m := make(map[string]struct{})
	for kpi, err := range packIndicies {
		if err != nil {
			return nil, err
		}
		for sha := range kpi.index {
			m[sha] = struct{}{}
		}
	}
	return m, nil
}
