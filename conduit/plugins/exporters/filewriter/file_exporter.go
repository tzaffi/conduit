package filewriter

import (
	"context"
	_ "embed" // used to embed config
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/sirupsen/logrus"

	"github.com/algorand/conduit/conduit/data"
	"github.com/algorand/conduit/conduit/plugins"
	"github.com/algorand/conduit/conduit/plugins/exporters"
)

const (
	// PluginName to use when configuring.
	PluginName = "file_writer"

	// FilePattern is used to name the output files.
	//FilePattern = "%[1]d_block.json"
	FilePattern = "%[1]d_block.msgp.gz"
)

type EncodingFormat byte

const (
	MessagepackFormat EncodingFormat = iota
	JSONFormat
	UnrecognizedFormat
)

type fileExporter struct {
	round  uint64
	cfg    Config
	gzip   bool
	format EncodingFormat
	logger *logrus.Logger
}

//go:embed sample.yaml
var sampleFile string

var metadata = plugins.Metadata{
	Name:         PluginName,
	Description:  "Exporter for writing data to a file.",
	Deprecated:   false,
	SampleConfig: sampleFile,
}

func (exp *fileExporter) Metadata() plugins.Metadata {
	return metadata
}


func (exp *fileExporter) Init(_ context.Context, initProvider data.InitProvider, cfg plugins.PluginConfig, logger *logrus.Logger) error {
	exp.logger = logger
	err := cfg.UnmarshalConfig(&exp.cfg)
	if err != nil {
		return fmt.Errorf("connect failure in unmarshalConfig: %w", err)
	}
	if exp.cfg.FilenamePattern == "" {
		exp.cfg.FilenamePattern = FilePattern
	}
	exp.gzip, exp.format, err = ParseFilenamePattern(exp.cfg.FilenamePattern)
	if err != nil {
		return fmt.Errorf("Init() error: %w", err)
	}

	// default to the data directory if no override provided.
	if exp.cfg.BlocksDir == "" {
		exp.cfg.BlocksDir = cfg.DataDir
	}
	// create block directory
	err = os.Mkdir(exp.cfg.BlocksDir, 0755)
	if err != nil && errors.Is(err, os.ErrExist) {
		// Ignore mkdir if the dir exists
		err = nil
	} else if err != nil {
		return fmt.Errorf("Init() error: %w", err)
	}
	exp.round = uint64(initProvider.NextDBRound())
	return err
}

func (exp *fileExporter) Close() error {
	exp.logger.Infof("latest round on file: %d", exp.round)
	return nil
}

func (exp *fileExporter) Receive(exportData data.BlockData) error {
	if exp.logger == nil {
		return fmt.Errorf("exporter not initialized")
	}
	if exportData.Round() != exp.round {
		return fmt.Errorf("Receive(): wrong block: received round %d, expected round %d", exportData.Round(), exp.round)
	}

	// write block to file
	{
		if exp.cfg.DropCertificate {
			exportData.Certificate = nil
		}

		blockFile := path.Join(exp.cfg.BlocksDir, fmt.Sprintf(exp.cfg.FilenamePattern, exportData.Round()))
		err := EncodeToFile(blockFile, &exportData, exp.format, exp.gzip)
		if err != nil {
			return fmt.Errorf("Receive(): failed to write file %s: %w", blockFile, err)	
		}


		// err := EncodeJSONToFile(blockFile, exportData, true)
		// if err != nil {
		// 	return fmt.Errorf("Receive(): failed to write file %s: %w", blockFile, err)
		// }
		exp.logger.Infof("Wrote block %d to %s", exportData.Round(), blockFile)
	}

	exp.round++
	return nil
}

func init() {
	exporters.Register(PluginName, exporters.ExporterConstructorFunc(func() exporters.Exporter {
		return &fileExporter{}
	}))
}
