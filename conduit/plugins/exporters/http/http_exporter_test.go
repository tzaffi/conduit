package http

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/algorand/conduit/conduit"
	"github.com/algorand/conduit/conduit/data"
	"github.com/algorand/conduit/conduit/plugins"
	"github.com/algorand/conduit/conduit/plugins/exporters"
	sdk "github.com/algorand/go-algorand-sdk/v2/types"
)

// TODO: this test is currently just a copy/pasta of the noop exporter test

var hc = exporters.ExporterConstructorFunc(func() exporters.Exporter {
	return &httpExporter{}
})
var he = hc.New()

func TestExporterBuilderByName(t *testing.T) {
	// init() has already registered the noop exporter
	assert.Contains(t, exporters.Exporters, metadata.Name)
	neBuilder, err := exporters.ExporterConstructorByName(metadata.Name)
	assert.NoError(t, err)
	ne := neBuilder.New()
	assert.Implements(t, (*exporters.Exporter)(nil), ne)
}

func TestExporterMetadata(t *testing.T) {
	meta := he.Metadata()
	assert.Equal(t, metadata.Name, meta.Name)
	assert.Equal(t, metadata.Description, meta.Description)
	assert.Equal(t, metadata.Deprecated, meta.Deprecated)
}

func TestExporterInit(t *testing.T) {
	assert.NoError(t, he.Init(context.Background(), &conduit.PipelineInitProvider{}, plugins.MakePluginConfig(""), nil))
}

func TestExporterClose(t *testing.T) {
	assert.NoError(t, he.Close())
}

func TestExporterRoundReceive(t *testing.T) {
	// TODO: to pass this test need to start up an http server that can handle the requests
	eData := data.BlockData{
		BlockHeader: sdk.BlockHeader{
			Round: 5,
		},
	}
	assert.NoError(t, he.Receive(eData))
}
