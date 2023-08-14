package http

import (
	"bytes"
	"context"
	_ "embed" // used to embed config
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/sirupsen/logrus"

	"github.com/algorand/conduit/conduit/data"
	"github.com/algorand/conduit/conduit/plugins"
	"github.com/algorand/conduit/conduit/plugins/exporters"
)

// PluginName to use when configuring.
var PluginName = "http"

// The `httpExporter` does not maintain state. It is assumed to be maintained
// by the external service.
type httpExporter struct {
	cfg      HTTPExporterConfig
	endpoint string
}

//go:embed sample.yaml
var sampleConfig string

var metadata = plugins.Metadata{
	Name:         PluginName,
	Description:  "HTTP exporter for exporting via an external web service.",
	Deprecated:   false,
	SampleConfig: sampleConfig,
}

func init() {
	exporters.Register(PluginName, exporters.ExporterConstructorFunc(func() exporters.Exporter {
		return &httpExporter{}
	}))
}

func (exp *httpExporter) Metadata() plugins.Metadata {
	return metadata
}

// Init only attempts to parse the config file.
// TODO: a configurable version might probe the health of the external service.
func (exp *httpExporter) Init(_ context.Context, _ data.InitProvider, cfg plugins.PluginConfig, _ *logrus.Logger) error {
	if err := cfg.UnmarshalConfig(&exp.cfg); err != nil {
		return fmt.Errorf("init failure in unmarshalConfig: %v", err)
	}
	exp.endpoint = fmt.Sprintf("http://localhost:%d%s", exp.cfg.Port, exp.cfg.Endpoint)
	return nil
}

type receivePayload struct {
	Round uint64          `json:"round"`
	Blk   *data.BlockData `json:"blk"`
	Empty bool            `json:"empty"`
}

// Close is a no-op for the httpExporter.
// TODO: a configurable, non-trivial version which calls the external service's close endpoint.
func (exp *httpExporter) Close() error {
	return nil
}

func (exp *httpExporter) Receive(exportData data.BlockData) error {
	round := exportData.Round()
	
	blkPtr := &exportData
	empty := false
	if round != 0 && exportData.Empty() {
		empty = true
		blkPtr = nil
	}
	payload := receivePayload{
		Round: round,
		Blk:   blkPtr,
		Empty: empty,
	}

	jsonData, err := json.Marshal(payload)
	// TODO: IncludedTransactions is giving me a marshalling _lint_ error.
	// https://github.com/algorand/go-algorand-sdk/blob/0865549639f0a359e2f79761203c58952d3b9532/types/statedelta.go#L224
	if err != nil {
		return err
	}

	resp, err := http.Post(exp.endpoint, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to post data to external endpoint: %w", err)
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		bodyBytes, err := io.ReadAll(resp.Body)
		return fmt.Errorf("http exporter failed with status code: %d, msg: %s read-err: %w", resp.StatusCode, string(bodyBytes), err)
	}

	return nil
}
