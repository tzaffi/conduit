package fileimporter

import (
	"context"
	_ "embed" // used to embed config
	"fmt"
	"os"
	"path"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/algorand/go-algorand-sdk/v2/encoding/json"
	sdk "github.com/algorand/go-algorand-sdk/v2/types"
	"github.com/algorand/go-codec/codec"

	"github.com/algorand/conduit/conduit/data"
	"github.com/algorand/conduit/conduit/plugins"
	"github.com/algorand/conduit/conduit/plugins/exporters/filewriter"
	"github.com/algorand/conduit/conduit/plugins/importers"
)

// PluginName to use when configuring.
const PluginName = "file_reader"

type fileReader struct {
	logger *logrus.Logger
	cfg    Config
	ctx    context.Context
	cancel context.CancelFunc
}

var jsonStrictHandle, prettyHandle *codec.JsonHandle

// package-wide init function
func init() {
	importers.Register(PluginName, importers.ImporterConstructorFunc(func() importers.Importer {
		return &fileReader{}
	}))

	// TODO: we need a common util for these encoding across
	// * http exporter
	// * file writer
	// * file reader
	prettyHandle = new(codec.JsonHandle)
	prettyHandle.ErrorIfNoField = json.CodecHandle.ErrorIfNoField
	prettyHandle.ErrorIfNoArrayExpand = json.CodecHandle.ErrorIfNoArrayExpand
	prettyHandle.Canonical = json.CodecHandle.Canonical
	prettyHandle.RecursiveEmptyCheck = json.CodecHandle.RecursiveEmptyCheck
	prettyHandle.Indent = json.CodecHandle.Indent
	prettyHandle.HTMLCharsAsIs = json.CodecHandle.HTMLCharsAsIs
	prettyHandle.MapKeyAsString = true
	prettyHandle.Indent = 2

	jsonStrictHandle = new(codec.JsonHandle)
	jsonStrictHandle.ErrorIfNoField = prettyHandle.ErrorIfNoField
	jsonStrictHandle.ErrorIfNoArrayExpand = prettyHandle.ErrorIfNoArrayExpand
	jsonStrictHandle.Canonical = prettyHandle.Canonical
	jsonStrictHandle.RecursiveEmptyCheck = prettyHandle.RecursiveEmptyCheck
	jsonStrictHandle.Indent = prettyHandle.Indent
	jsonStrictHandle.HTMLCharsAsIs = prettyHandle.HTMLCharsAsIs
	jsonStrictHandle.MapKeyAsString = true

}

// New initializes an algod importer
func New() importers.Importer {
	return &fileReader{}
}

//go:embed sample.yaml
var sampleConfig string

var metadata = plugins.Metadata{
	Name:         PluginName,
	Description:  "Importer for fetching blocks from files in a directory created by the 'file_writer' plugin.",
	Deprecated:   false,
	SampleConfig: sampleConfig,
}

func (r *fileReader) Metadata() plugins.Metadata {
	return metadata
}

func (r *fileReader) Init(ctx context.Context, _ data.InitProvider, cfg plugins.PluginConfig, logger *logrus.Logger) error {
	r.ctx, r.cancel = context.WithCancel(ctx)
	r.logger = logger
	err := cfg.UnmarshalConfig(&r.cfg)
	if err != nil {
		return fmt.Errorf("invalid configuration: %v", err)
	}

	if r.cfg.FilenamePattern == "" {
		r.cfg.FilenamePattern = filewriter.FilePattern
	}

	return nil
}

func (r *fileReader) GetGenesis() (*sdk.Genesis, error) {
	genesisFile := path.Join(r.cfg.BlocksDir, "genesis.json")
	var genesis sdk.Genesis
	err := filewriter.DecodeJSONFromFile(genesisFile, &genesis, false)
	if err != nil {
		return nil, fmt.Errorf("GetGenesis(): failed to process genesis file: %w", err)
	}
	return &genesis, nil
}

func (r *fileReader) Close() error {
	if r.cancel != nil {
		r.cancel()
	}
	return nil
}

// func (r *fileReader) GetBlock(rnd uint64) (data.BlockData, error) {
// 	filename := path.Join(r.cfg.BlocksDir, fmt.Sprintf(r.cfg.FilenamePattern, rnd))
// 	var blockData data.BlockData
// 	start := time.Now()
// 	err := filewriter.DecodeJSONFromFile(filename, &blockData, false)
// 	if err != nil {
// 		return data.BlockData{}, fmt.Errorf("GetBlock(): unable to read block file '%s': %w", filename, err)
// 	}
// 	r.logger.Infof("Block %d read time: %s", rnd, time.Since(start))
// 	return blockData, nil
// }

func (r *fileReader) GetBlock(rnd uint64) (data.BlockData, error) {
	filename := path.Join(r.cfg.BlocksDir, fmt.Sprintf(r.cfg.FilenamePattern, rnd))
	var blockData data.BlockData
	start := time.Now()

	// Read file content
	fileContent, err := os.ReadFile(filename)
	if err != nil {
		return data.BlockData{}, fmt.Errorf("GetBlock(): unable to read block file '%s': %w", filename, err)
	}

	// Decode using jsonStrictHandle
	decoder := codec.NewDecoderBytes(fileContent, jsonStrictHandle)
	err = decoder.Decode(&blockData)
	if err != nil {
		return data.BlockData{}, fmt.Errorf("GetBlock(): unable to decode block file '%s': %w", filename, err)
	}

	r.logger.Infof("Block %d read time: %s", rnd, time.Since(start))
	return blockData, nil
}
