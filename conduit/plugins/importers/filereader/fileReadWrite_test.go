package fileimporter

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"testing"

	logrusTest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/algorand/conduit/conduit"
	"github.com/algorand/conduit/conduit/data"
	"github.com/algorand/conduit/conduit/pipeline"
	"github.com/algorand/conduit/conduit/plugins"
	"github.com/algorand/conduit/conduit/plugins/exporters"
	"github.com/algorand/conduit/conduit/plugins/exporters/filewriter"
	"github.com/algorand/conduit/conduit/plugins/importers"
	sdk "github.com/algorand/go-algorand-sdk/v2/types"
)

const (
	conduitDataDir   = "test_resources/conduit_data"
	filePattern      = "%[1]d_block.msgp.gz"
	importerBlockDir = "test_resources/filereader_blocks"
	exporterBlockDir = "test_resources/conduit_data/exporter_file_writer"
)

// numFiles returns the number of files in the importerBlockDir including genesis.
// This assumes that there are no subdirectories.
func numFiles(t *testing.T) uint64 {
	files, err := os.ReadDir(importerBlockDir)
	require.NoError(t, err)

	return uint64(len(files))
}

func fileInfo(t *testing.T, path string) (os.FileInfo, *os.File){
	fileInfo, err := os.Stat(path)
	require.NoError(t, err, "error accessing file %s", path)

	file, err := os.Open(path)
	require.NoError(t, err, "error opening file %s", path)

	return fileInfo, file
}


func identicalFiles(t *testing.T, path1, path2 string) {
	var fileInfo1, fileInfo2 os.FileInfo
	var file1, file2 *os.File

	defer func() {
		if file1 != nil {
			file1.Close()
		}
		if file2 != nil {
			file2.Close()
		}
	}()

	fileInfo1, file1 = fileInfo(t, path1)
	fileInfo2, file2 = fileInfo(t, path2)
	require.Equal(t, fileInfo1.Size(), fileInfo2.Size())

	buffer1 := make([]byte, 4096)
	buffer2 := make([]byte, 4096)

	for idx := 0; ; idx++{
		msg := fmt.Sprintf("files: %s v %s, buffer beginning at offest %d", path1, path2, 4096 * idx)
		n1, err1 := io.ReadFull(file1, buffer1)
		n2, err2 := io.ReadFull(file2, buffer2)

		require.Equal(t, n1, n2, msg)

		if err1 != nil {
			require.ErrorIs(t, err1, io.EOF, msg)
			require.ErrorIs(t, err2, io.EOF, msg)
		}

		require.Equal(t, buffer1[:n1], buffer2[:n2], msg)
	}
}


// func pluginConfig(t *testing.T, cfg data.NameConfigPair, dataDir string) plugins.PluginConfig {
// 	config, err := yaml.Marshal(cfg.Config)
// 	require.NoError(t, err)

// 	return plugins.PluginConfig{
// 		DataDir: dataDir,
// 		Config:  string(config),
// 	}
// }

// TestRoundTrip tests that blocks read by the filereader importer
// under the msgp.gz encoding are written to identical files by the filewriter exporter.
// This includes both a genesis block and a round-0 block with differend encodings.
func TestRoundTrip(t *testing.T) {
	round := sdk.Round(0)
	lastRound := numFiles(t) - 2 // subtracd round-0 and the separate genesis file
	require.GreaterOrEqual(t, lastRound, uint64(1))
	require.LessOrEqual(t, lastRound, uint64(1000)) // overflow sanity check

	ctx := context.Background()

	plineConfig, err := data.MakePipelineConfig(&data.Args{
		ConduitDataDir: conduitDataDir,
	})
	require.NoError(t, err)

	logger, hook := logrusTest.NewNullLogger()
	// TODO: should we use the hook to assert logs?
	_ = hook


	// Assert configurations:
	require.Equal(t, "file_reader", plineConfig.Importer.Name)
	require.Equal(t, importerBlockDir, plineConfig.Importer.Config["block-dir"])
	require.Equal(t, filePattern, plineConfig.Importer.Config["filename-pattern"])

	require.Equal(t, "file_writer", plineConfig.Exporter.Name)
	require.Equal(t, filePattern, plineConfig.Exporter.Config["filename-pattern"])
	require.False(t, plineConfig.Exporter.Config["drop-certificate"].(bool))

	// Simulate the portions of the pipeline's Init() that interact
	// with the importer and exporter
	initProvider := conduit.MakePipelineInitProvider(&round, nil, nil)

	// Importer init
	impCtor, err := importers.ImporterConstructorByName(plineConfig.Importer.Name)
	require.NoError(t, err)
	importer := impCtor.New()
	impConfig, err := pipeline.MakePluginConfig(plineConfig.ConduitArgs, plineConfig.Importer, plugins.Importer)
	require.NoError(t, err)
	require.Equal(t, path.Join(conduitDataDir, "importer_file_reader"), impConfig.DataDir)

	err = importer.Init(ctx, initProvider, impConfig, logger)
	require.NoError(t, err)

	impGenesis, err := importer.GetGenesis()
	require.NoError(t, err)
	require.Equal(t, "generated-network", impGenesis.Network)

	// it should be the same as unmarshalling it directly from the expected path
	genesisFile, err := filewriter.GenesisFilename(filewriter.MessagepackFormat, true)
	require.Equal(t, "genesis.msgp.gz", genesisFile)
	require.NoError(t, err)

	impGenesisPath := path.Join(importerBlockDir, genesisFile)
	genesis := &sdk.Genesis{}

	err = filewriter.DecodeFromFile(impGenesisPath, genesis, filewriter.MessagepackFormat, true)
	require.NoError(t, err)

	require.Equal(t, impGenesis, genesis)


	initProvider.SetGenesis(impGenesis)

	// Construct the exporter
	expCtor, err := exporters.ExporterConstructorByName(plineConfig.Exporter.Name)
	require.NoError(t, err)
	exporter := expCtor.New()
	expConfig, err := pipeline.MakePluginConfig(plineConfig.ConduitArgs, plineConfig.Exporter, plugins.Exporter)
	require.NoError(t, err)
	require.Equal(t, path.Join(conduitDataDir, "exporter_file_writer"), expConfig.DataDir)

	err = exporter.Init(ctx, initProvider, expConfig, logger)
	require.NoError(t, err)

	// It should have persisted the genesis which ought to be identical
	// to the importer's.
	expGenesisPath := path.Join(exporterBlockDir, genesisFile)
	identicalFiles(t, impGenesisPath, expGenesisPath)
	


	// Run the pipeline
	require.Equal(t, sdk.Round(0), round)
	for ; uint64(round) <= lastRound; round++ {
		blk, err := importer.GetBlock(uint64(round))
		require.NoError(t, err)

		if round == 0 {
			require.Equal(t, impGenesis, blk)
		}

		file, err := os.OpenFile(exporterBlockDir + "/" + fmt.Sprintf(filePattern, round), os.O_RDONLY, 0)
		require.ErrorIs(t, err, os.ErrNotExist)
		_ = file

	}


}
