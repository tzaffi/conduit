package algodimporter

import (
	"bufio"
	"context"
	_ "embed" // used to embed config
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/algod"
	"github.com/algorand/go-algorand-sdk/v2/client/v2/common"
	"github.com/algorand/go-algorand-sdk/v2/client/v2/common/models"
	"github.com/algorand/go-algorand-sdk/v2/encoding/json"
	"github.com/algorand/go-algorand-sdk/v2/encoding/msgpack"
	sdk "github.com/algorand/go-algorand-sdk/v2/types"

	"github.com/algorand/conduit/conduit/data"
	"github.com/algorand/conduit/conduit/plugins"
	"github.com/algorand/conduit/conduit/plugins/importers"
)

const (
	// PluginName to use when configuring.
	PluginName      = "algod"
	archivalModeStr = "archival"
	followerModeStr = "follower"
)

const (
	archivalMode = iota
	followerMode
)

// Retry
const (
	retries = 5
)

const catchpointsURL = "https://algorand-catchpoints.s3.us-east-2.amazonaws.com/consolidated/%s_catchpoints.txt"

type algodImporter struct {
	aclient *algod.Client
	logger  *logrus.Logger
	cfg     Config
	ctx     context.Context
	cancel  context.CancelFunc
	mode    int
}

//go:embed sample.yaml
var sampleConfig string

var algodImporterMetadata = plugins.Metadata{
	Name:         PluginName,
	Description:  "Importer for fetching blocks from an algod REST API.",
	Deprecated:   false,
	SampleConfig: sampleConfig,
}

func (algodImp *algodImporter) OnComplete(input data.BlockData) error {
	if algodImp.mode == followerMode {
		_, err := algodImp.aclient.SetSyncRound(input.Round() + 1).Do(algodImp.ctx)
		return err
	}

	return nil
}

func (algodImp *algodImporter) Metadata() plugins.Metadata {
	return algodImporterMetadata
}

// package-wide init function
func init() {
	importers.Register(PluginName, importers.ImporterConstructorFunc(func() importers.Importer {
		return &algodImporter{}
	}))
}

func parseCatchpointRound(catchpoint string) (round sdk.Round, err error) {
	parts := strings.Split(catchpoint, "#")
	if len(parts) != 2 {
		err = fmt.Errorf("unable to parse catchpoint, invalid format: %s", catchpoint)
		return
	}
	uiRound, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return
	}
	round = sdk.Round(uiRound)
	return
}

func (algodImp *algodImporter) startCatchpointCatchup(catchpoint string) error {
	// Run catchpoint catchup
	client, err := common.MakeClient(algodImp.cfg.NetAddr, "X-Algo-API-Token", algodImp.cfg.CatchupConfig.AdminToken)
	if err != nil {
		return fmt.Errorf("received unexpected error creating catchpoint client: %w", err)
	}
	var resp string
	err = client.Post(
		algodImp.ctx,
		&resp,
		fmt.Sprintf("/v2/catchup/%s", common.EscapeParams(catchpoint)...),
		nil,
		nil,
		nil,
	)
	if err != nil {
		return fmt.Errorf("POST /v2/catchup/%s received unexpected error: %w", catchpoint, err)
	}
	return nil
}

func (algodImp *algodImporter) monitorCatchpointCatchup() error {
	// Delay is the time to wait for catchup service startup
	var delay = 5 * time.Second
	start := time.Now()
	// Report progress periodically while waiting for catchup to complete
	running := true
	for running {
		select {
		case <-time.After(delay):
		case <-algodImp.ctx.Done():
			return algodImp.ctx.Err()
		}
		stat, err := algodImp.aclient.Status().Do(algodImp.ctx)
		if err != nil {
			return fmt.Errorf("received unexpected error getting node status: %w", err)
		}
		running = stat.Catchpoint != ""
		switch {
		case !running:
			break
		case stat.CatchpointAcquiredBlocks > 0:
			algodImp.logger.Infof("catchup phase Acquired Blocks: %d / %d", stat.CatchpointAcquiredBlocks, stat.CatchpointTotalBlocks)
		case stat.CatchpointVerifiedAccounts > 0:
			algodImp.logger.Infof("catchup phase Verified Accounts: %d / %d", stat.CatchpointVerifiedAccounts, stat.CatchpointTotalAccounts)
		case stat.CatchpointProcessedAccounts > 0:
			algodImp.logger.Infof("catchup phase Processed Accounts: %d / %d", stat.CatchpointProcessedAccounts, stat.CatchpointTotalAccounts)
		default:
			// Todo: We should expose catchupService.VerifiedBlocks via the NodeStatusResponse
			algodImp.logger.Infof("catchup phase Verified Blocks")
		}

	}

	algodImp.logger.Infof("Catchpoint catchup finished in %s", time.Since(start))
	return nil
}

func getMissingCatchpointLabel(URL string, nextRound uint64) (string, error) {
	resp, err := http.Get(URL)
	if err != nil {
		return "", err
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read catchpoint label response: %w", err)
	}

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("failed to lookup catchpoint label list (%d): %s", resp.StatusCode, string(body))
	}

	// look for best match without going over
	var label string
	labels := string(body)
	scanner := bufio.NewScanner(strings.NewReader(labels))
	for scanner.Scan() && scanner.Text() != "" {
		line := scanner.Text()
		round, err := parseCatchpointRound(line)
		if err != nil {
			return "", err
		}
		// TODO: Change >= to > after go-algorand#5352 is fixed.
		if uint64(round) >= nextRound {
			break
		}
		label = line
	}

	if label == "" {
		return "", fmt.Errorf("no catchpoint label found for round %d at: %s", nextRound, URL)
	}

	return label, nil
}

// checkRounds to see if catchup is needed, an error is returned if a bad state
// is detected.
func checkRounds(logger *logrus.Logger, catchpointRound, nodeRound, targetRound uint64) (bool, error) {
	// Make sure catchpoint round is not in the future
	// TODO: Change < to <= after go-algorand#5352 is fixed.
	canCatchup := catchpointRound < targetRound
	mustCatchup := targetRound < nodeRound
	shouldCatchup := nodeRound < catchpointRound

	msg := fmt.Sprintf("Node round %d, target round %d, catchpoint round %d", nodeRound, targetRound, catchpointRound)

	if canCatchup && mustCatchup {
		logger.Infof("Catchup required, node round ahead of target round. %s.", msg)
		return true, nil
	}

	if canCatchup && shouldCatchup {
		logger.Infof("Catchup requested. %s.", msg)
		return true, nil
	}

	if !canCatchup && mustCatchup {
		err := fmt.Errorf("node round %d and catchpoint round %d are ahead of target round %d", nodeRound, catchpointRound, targetRound)
		logger.Errorf("Catchup required but no valid catchpoint available, %s.", err.Error())
		return false, err
	}

	logger.Infof("No catchup required. %s.", msg)
	return false, nil
}

func (algodImp *algodImporter) needsCatchup(targetRound uint64) bool {
	if algodImp.mode == followerMode {
		if targetRound == 0 {
			algodImp.logger.Info("No state deltas are ever available for round 0")
			return true
		}
		// If we are in follower mode, check if the round delta is available.
		_, err := algodImp.getDelta(targetRound)
		if err != nil {
			algodImp.logger.Infof("Unable to fetch state delta for round %d: %s", targetRound, err)
		}
		return err != nil
	}

	// Otherwise just check if the block is available.
	_, err := algodImp.aclient.Block(targetRound).Do(algodImp.ctx)
	if err != nil {
		algodImp.logger.Infof("Unable to fetch block for round %d: %s", targetRound, err)
	}
	// If the block is not available, we must catchup.
	return err != nil
}

// catchupNode facilitates catching up via fast catchup, or waiting for the
// node to slow catchup.
func (algodImp *algodImporter) catchupNode(network string, targetRound uint64) error {
	if !algodImp.needsCatchup(targetRound) {
		algodImp.logger.Infof("No catchup required to reach round %d", targetRound)
		return nil
	}

	algodImp.logger.Infof("Catchup required to reach round %d", targetRound)

	catchpoint := ""

	// If there is an admin token, look for a catchpoint to use.
	if algodImp.cfg.CatchupConfig.AdminToken != "" {
		if algodImp.cfg.CatchupConfig.Catchpoint != "" {
			catchpoint = algodImp.cfg.CatchupConfig.Catchpoint
		} else {
			URL := fmt.Sprintf(catchpointsURL, network)
			var err error
			catchpoint, err = getMissingCatchpointLabel(URL, targetRound)
			if err != nil {
				return fmt.Errorf("unable to lookup catchpoint: %w", err)
			}
		}
	}

	if catchpoint != "" {
		cpRound, err := parseCatchpointRound(catchpoint)
		if err != nil {
			return err
		}

		// Get the node status.
		nStatus, err := algodImp.aclient.Status().Do(algodImp.ctx)
		if err != nil {
			return fmt.Errorf("received unexpected error failed to get node status: %w", err)
		}

		if runCatchup, err := checkRounds(algodImp.logger, uint64(cpRound), nStatus.LastRound, targetRound); !runCatchup || err != nil {
			return err
		}
		algodImp.logger.Infof("Starting catchpoint catchup with label %s", catchpoint)

		err = algodImp.startCatchpointCatchup(catchpoint)
		if err != nil {
			return err
		}

		// Wait for algod to catchup
		err = algodImp.monitorCatchpointCatchup()
		if err != nil {
			return err
		}
	}

	// Set the sync round after fast-catchup in case the node round is ahead of the target round.
	// Trying to set it before would cause an error.
	if algodImp.mode == followerMode {
		// Set the sync round to the round provided by initProvider
		_, err := algodImp.aclient.SetSyncRound(targetRound).Do(algodImp.ctx)
		if err != nil {
			return fmt.Errorf("received unexpected error setting sync round (%d): %w", targetRound, err)
		}
	}

	_, err := algodImp.aclient.StatusAfterBlock(targetRound).Do(algodImp.ctx)
	if err != nil {
		err = fmt.Errorf("received unexpected error (StatusAfterBlock) waiting for node to catchup: %w", err)
	}
	return err
}

func (algodImp *algodImporter) Init(ctx context.Context, initProvider data.InitProvider, cfg plugins.PluginConfig, logger *logrus.Logger) (*sdk.Genesis, error) {
	algodImp.ctx, algodImp.cancel = context.WithCancel(ctx)
	algodImp.logger = logger
	err := cfg.UnmarshalConfig(&algodImp.cfg)
	if err != nil {
		return nil, fmt.Errorf("connect failure in unmarshalConfig: %v", err)
	}

	// To support backwards compatibility with the daemon we default to archival mode
	if algodImp.cfg.Mode == "" {
		algodImp.cfg.Mode = archivalModeStr
	}

	switch algodImp.cfg.Mode {
	case archivalModeStr:
		algodImp.mode = archivalMode
	case followerModeStr:
		algodImp.mode = followerMode
	default:
		return nil, fmt.Errorf("algod importer was set to a mode (%s) that wasn't supported", algodImp.cfg.Mode)
	}

	var client *algod.Client
	u, err := url.Parse(algodImp.cfg.NetAddr)
	if err != nil {
		return nil, err
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		algodImp.cfg.NetAddr = "http://" + algodImp.cfg.NetAddr
		algodImp.logger.Infof("Algod Importer added http prefix to NetAddr: %s", algodImp.cfg.NetAddr)
	}
	client, err = algod.MakeClient(algodImp.cfg.NetAddr, algodImp.cfg.Token)
	if err != nil {
		return nil, err
	}
	algodImp.aclient = client

	genesisResponse, err := client.GetGenesis().Do(ctx)
	if err != nil {
		return nil, err
	}

	genesis := sdk.Genesis{}

	// Don't fail on unknown properties here since the go-algorand and SDK genesis types differ slightly
	err = json.LenientDecode([]byte(genesisResponse), &genesis)
	if err != nil {
		return nil, err
	}
	if reflect.DeepEqual(genesis, sdk.Genesis{}) {
		return nil, fmt.Errorf("unable to fetch genesis file from API at %s", algodImp.cfg.NetAddr)
	}

	err = algodImp.catchupNode(genesis.Network, uint64(initProvider.NextDBRound()))

	return &genesis, err
}

// func (algodImp *algodImporter) Config() string {
// 	s, _ := yaml.Marshal(algodImp.cfg)
// 	return string(s)
// }

func (algodImp *algodImporter) Close() error {
	if algodImp.cancel != nil {
		algodImp.cancel()
	}
	return nil
}

func (algodImp *algodImporter) getDelta(rnd uint64) (sdk.LedgerStateDelta, error) {
	var delta sdk.LedgerStateDelta
	params := struct {
		Format string `url:"format,omitempty"`
	}{Format: "msgp"}
	// Note: this uses lenient decoding. GetRaw and msgpack.Decode would allow strict decoding.
	err := (*common.Client)(algodImp.aclient).GetRawMsgpack(algodImp.ctx, &delta, fmt.Sprintf("/v2/deltas/%d", rnd), params, nil)
	if err != nil {
		return sdk.LedgerStateDelta{}, err
	}

	return delta, nil
}

func (algodImp *algodImporter) GetBlock(rnd uint64) (data.BlockData, error) {
	var blockbytes []byte
	var err error
	var status models.NodeStatus
	var blk data.BlockData

	for r := 0; r < retries; r++ {
		status, err = algodImp.aclient.StatusAfterBlock(rnd - 1).Do(algodImp.ctx)
		if err != nil {
			// If context has expired.
			if algodImp.ctx.Err() != nil {
				return blk, fmt.Errorf("GetBlock ctx error: %w", err)
			}
			err = fmt.Errorf("error getting status for round: %w", err)
			algodImp.logger.Errorf("error getting status for round %d (attempt %d)", rnd, r)
			continue
		}
		start := time.Now()
		blockbytes, err = algodImp.aclient.BlockRaw(rnd).Do(algodImp.ctx)
		dt := time.Since(start)
		getAlgodRawBlockTimeSeconds.Observe(dt.Seconds())
		if err != nil {
			algodImp.logger.Errorf("error getting block for round %d (attempt %d)", rnd, r)
			continue
		}
		tmpBlk := new(models.BlockResponse)
		err = msgpack.Decode(blockbytes, tmpBlk)
		if err != nil {
			return blk, err
		}

		blk.BlockHeader = tmpBlk.Block.BlockHeader
		blk.Payset = tmpBlk.Block.Payset
		blk.Certificate = tmpBlk.Cert

		if algodImp.mode == followerMode {
			// Round 0 has no delta associated with it
			if rnd != 0 {
				var delta sdk.LedgerStateDelta
				delta, err = algodImp.getDelta(rnd)
				if err != nil {
					if status.LastRound < rnd {
						err = fmt.Errorf("ledger state delta not found: node round (%d) is behind required round (%d), ensure follower node has its sync round set to the required round: %w", status.LastRound, rnd, err)
					} else {
						err = fmt.Errorf("ledger state delta not found: node round (%d), required round (%d): verify follower node configuration and ensure follower node has its sync round set to the required round, re-deploying the follower node may be necessary: %w", status.LastRound, rnd, err)
					}
					algodImp.logger.Error(err.Error())
					return data.BlockData{}, err
				}
				blk.Delta = &delta
			}
		}

		return blk, err
	}

	err = fmt.Errorf("failed to get block for round %d after %d attempts, check node configuration: %s", rnd, retries, err)
	algodImp.logger.Errorf(err.Error())
	return blk, err
}

func (algodImp *algodImporter) ProvideMetrics(subsystem string) []prometheus.Collector {
	getAlgodRawBlockTimeSeconds = initGetAlgodRawBlockTimeSeconds(subsystem)
	return []prometheus.Collector{
		getAlgodRawBlockTimeSeconds,
	}
}
