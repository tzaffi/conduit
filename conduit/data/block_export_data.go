package data

import (
	sdk "github.com/algorand/go-algorand-sdk/v2/types"
)

// RoundProvider is the interface which all data types sent to Exporters should implement
type RoundProvider interface {
	Round() uint64
	Empty() bool
}

// InitProvider is the interface that can be used when initializing to get common algod related
// variables
type InitProvider interface {
	GetGenesis() *sdk.Genesis
	SetGenesis(*sdk.Genesis)
	NextDBRound() sdk.Round
}

// BlockData is provided to the Exporter on each round.
type BlockData struct {

	// BlockHeader is the immutable header from the block
	BlockHeader sdk.BlockHeader `json:"block,omitempty"`

	// Payset is the set of data the block is carrying--can be modified as it is processed
	Payset []sdk.SignedTxnInBlock `json:"payset,omitempty"`

	// Delta contains a list of account changes resulting from the block. Processor plugins may have modify this data.
	Delta *sdk.LedgerStateDelta `json:"delta,omitempty"`

	// Certificate contains voting data that certifies the block. The certificate is non deterministic, a node stops collecting votes once the voting threshold is reached.
	Certificate *map[string]interface{} `json:"cert,omitempty"`
}

// Round returns the round to which the BlockData corresponds
func (blkData BlockData) Round() uint64 {
	return uint64(blkData.BlockHeader.Round)
}

// Empty returns whether the Block contains Txns. Assumes the Block is never nil
func (blkData BlockData) Empty() bool {
	return len(blkData.Payset) == 0
}
