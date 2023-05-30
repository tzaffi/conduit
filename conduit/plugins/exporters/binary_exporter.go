package exporters

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os/exec"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"

	"github.com/algorand/conduit/conduit/data"
	"github.com/algorand/conduit/conduit/plugins"
)

// BinaryExporter is a struct that contains the configuration for the binary exporter.
type BinaryExporter struct {
	CommandPath string

	MetadataSubCommand string // default: "metadata"

	DaemonSubCommand string // default: "daemon"
	ConfigArg        string // default: "--config"
	GenesisArg       string // default: "--genesis"
	InitRoundArg     string // default: "--init-round"

	ConduitConfigCommand string // default: "config"

	ConduitReceiveCommand    string // default: "receive"
	ConduitReceiveCommandArg string // default: "exportData"

	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.Reader
	stderr io.Reader
	logger *logrus.Logger
	ctx    context.Context
	cf     context.CancelFunc
}

// MakeBinaryExporter creates a BinaryExporter with default values.
// TODO: allow more customization.
func MakeBinaryExporter(commandPath string) *BinaryExporter {
	return &BinaryExporter{
		CommandPath:              commandPath,
		MetadataSubCommand:       "metadata",
		DaemonSubCommand:         "daemon",
		ConfigArg:                "--config",
		GenesisArg:               "--genesis",
		InitRoundArg:             "--init-round",
		ConduitConfigCommand:     "config",
		ConduitReceiveCommand:    "receive",
		ConduitReceiveCommandArg: "exportData",
	}
}

// Metadata returns the metadata for the binary exporter.
func (exp *BinaryExporter) Metadata() plugins.Metadata {
	m, err := exp.getMetadata()
	if err != nil {
		// If we couldn't retrieve metadata, return default metadata
		return plugins.Metadata{
			Name:         "Unknown",
			Description:  "Unknown",
			Deprecated:   false,
			SampleConfig: "{}",
		}
	}

	return m
}

func (exp *BinaryExporter) getMetadata() (plugins.Metadata, error) {
	cmd := exec.Command(exp.CommandPath, exp.MetadataSubCommand)
	outputBytes, err := cmd.Output()
	if err != nil {
		return plugins.Metadata{}, err
	}
	output := strings.TrimSuffix(string(outputBytes), "\n")

	var m plugins.Metadata
	err = json.Unmarshal([]byte(output), &m)
	if err != nil {
		return plugins.Metadata{}, err
	}

	var s map[string]interface{}
	err = yaml.Unmarshal([]byte(m.SampleConfig), &s)
	if err != nil {
		return plugins.Metadata{}, err
	}

	// Then we marshal the map to YAML
	yamlData, err := yaml.Marshal(&s)
	if err != nil {
		return plugins.Metadata{}, err
	}
	m.SampleConfig = string(yamlData)
	m.SampleConfig = "  " + strings.ReplaceAll(m.SampleConfig, "\n", "\n  ")
	return m, nil
}

// Config returns the config for the binary exporter.
func (exp *BinaryExporter) Config() string {
	_, err := exp.stdin.Write([]byte("{\"conduitCommand\": \"config\"}\n"))
	if err != nil {
		// log error or handle it as per your needs
		return ""
	}

	reader := bufio.NewReader(exp.stdout)
	config, _, err := reader.ReadLine()
	if err != nil {
		// log error or handle it as per your needs
		return ""
	}

	return string(config)
}

// Close attempts to close the binary exporter.
func (exp *BinaryExporter) Close() error {
	// TODO: kill the daemon if it's still running
	_, err := exp.stdin.Write([]byte("{\"conduitCommand\": \"close\"}\n"))
	return err
}

func marshalExportData(exportData data.BlockData) (string, error) {
	jsonData, err := json.Marshal(exportData.BlockHeader.Branch)
	if err != nil {
		return "", err
	}
	return string(jsonData), nil
}

// Init initializes the binary exporter.
func (exp *BinaryExporter) Init(ctx context.Context, initProvider data.InitProvider, cfg plugins.PluginConfig, logger *logrus.Logger) error {
	exp.ctx, exp.cf = context.WithCancel(ctx)
	exp.logger = logger

	genesis, err := json.Marshal(initProvider.GetGenesis())
	if err != nil {
		return err
	}

	var yamlConfig interface{}
	err = yaml.Unmarshal([]byte(cfg.Config), &yamlConfig)
	if err != nil {
		return err
	}
	jsonConfig, err := json.Marshal(yamlConfig)
	if err != nil {
		return err
	}

	initRound := initProvider.NextDBRound()

	exp.cmd = exec.Command("block_hash_exporter.sh", "daemon", "--config", string(jsonConfig), "--genesis", string(genesis), "--init-round", strconv.Itoa(int(initRound)))
	exp.stdin, _ = exp.cmd.StdinPipe()
	exp.stdout, _ = exp.cmd.StdoutPipe()
	exp.stderr, _ = exp.cmd.StderrPipe()

	err = exp.cmd.Start()
	if err != nil {
		return err
	}

	done := make(chan bool)

	// Setup goroutine to read stdout
	go func() {
		scanner := bufio.NewScanner(exp.stdout)
		for scanner.Scan() {
			logger.Info("STDOUT: ", scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			logger.Error("Reading standard output:", err)
			done <- true
		}
	}()

	// Setup goroutine to read stderr
	go func() {
		scanner := bufio.NewScanner(exp.stderr)
		for scanner.Scan() {
			logger.Error("STDERR: ", scanner.Text())
			done <- true
		}
		if err := scanner.Err(); err != nil {
			logger.Error("Reading standard error:", err)
			done <- true
		}
	}()

	go func() {
		<-done
		exp.stdin.Close()
	}()

	return nil
}

// Receive a block from the processor and export it.
func (exp *BinaryExporter) Receive(exportData data.BlockData) error {
	jsonExportData, err := marshalExportData(exportData)
	if err != nil {
		return err
	}
	_, err = exp.stdin.Write([]byte("{\"conduitCommand\": \"receive\", \"args\": {\"exportData\": " + jsonExportData + "}}\n"))
	return err
}
