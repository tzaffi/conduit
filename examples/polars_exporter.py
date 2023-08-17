import base64
from dataclasses import asdict, dataclass, fields, is_dataclass
import os
from pathlib import Path
# import shutil

from flask import Flask, request, jsonify
from glom import glom, Path as GPath
import msgpack
import polars as pl
from typing import cast


SCRIPT_DIR = Path(__file__).parent
DELTA_LAKE_TXNS = SCRIPT_DIR / "pypolars_data" / "exporter_http" / "txns.parquet"
EXCLUDES = {"gd"}
PROBLEM_COLS = ["dt_gd"]
TXN_DF: pl.DataFrame | None = None

app = Flask(__name__)


@app.route("/receive", methods=["POST"])
def receive():
    content_type = request.headers.get("Content-Type")

    receive_payload = None
    if content_type == "application/msgpack":
        print("handling msgpack")
        try:
            receive_payload = base64.b64decode(request.data)
            receive_payload = msgpack.unpackb(
                receive_payload, raw=True, strict_map_key=False
            )
            print(f"{receive_payload[b'round']=}, {receive_payload[b'empty']=}")
        except Exception as e:
            print(f"error: {e}")
            return jsonify({"error": str(e)}), 400

    elif content_type == "application/json":
        print("handling json")
        try:
            receive_payload = request.json
        except Exception as e:
            print(f"error: {e}")
            return jsonify({"error": str(e)}), 400
    else:
        return jsonify({"error": "Unsupported content type"}), 400

    try:
        rows = save_txns(receive_payload, content_type)
        return jsonify({"success": True, "rows": rows}), 200
    except Exception as e:
        print(f"error: {e}")
        return jsonify({"error": str(e)}), 400


def polars_decode(data: bytes) -> str:
    assert isinstance(data, bytes), f"Expected bytes, got {type(data)}"
    try:
        return "U" + data.decode("utf-8")
    except UnicodeDecodeError:
        return "B" + base64.b64encode(data).decode("utf-8")


def polars_encode(data: str) -> bytes:
    assert isinstance(data, str), f"Expected Utf8, got {type(data)}"
    assert len(data) > 0, f"Expected non-empty string, got {data}"
    assert data[0] in (
        "U",
        "B",
    ), f"Expected Utf8 to start with 'U' or 'B', got {data[0]}"

    if data[0] == "U":
        return data[1:].encode("utf-8")
    return base64.b64decode(data[1:])


def save_txns(receive_payload: dict, content_type: str) -> int:
    assert (
        content_type == "application/msgpack"
    ), f"Unsupported content type {content_type}"
    round = receive_payload[b"round"]
    blk = receive_payload[b"blk"]
    block = blk[b"block"]
    payset = blk.get(b"payset")
    delta = blk.get(b"delta")
    # genesis has:
    # * blk.keys()
    #       --> [b'block']
    # * block.keys()
    #       --> [b'fees', b'gen', b'gh', b'proto', b'rwcalr', b'rwd', b'seed', b'tc', b'ts', b'txn']
    # * block[b'txn'].hex() == '277862b1b2d2d1279bb5a19d0d87518fe71500f126b8ba336775bd349a1e7b73'
    # maybe this is the genesis txn id?
    #
    # regular blocks have:
    # * blk.keys()
    #       --> [b'block', b'delta', b'payset']
    # * block.keys() (missing: 'rwcalr', 'seed', 'txn'; adding 'rnd')
    #       --> [b'fees', b'gen', b'gh', b'proto', b'rnd', b'rwd', b'tc', b'ts']
    # * payset[i].keys() are subsets of [b'apid', b'hgi', b'sig', b'txn', b'hgi', b'dt' ... ]
    # * delta.keys()
    #       --> [b'Accts', b'Creatables', b'Hdr', b'KvMods', b'PrevTimestamp', b'StateProofNext', b'Totals', b'Txids', b'Txleases']
    # rows = len(block[b"txns"])
    # return rows
    rows = save_payset(payset, round)
    print(
        f"save_txns(): {round=}, {len(block)=}, {bool(payset)=}, {bool(delta)=}, {rows=}"
    )
    return rows


@dataclass
class IndexedTxn:
    index: list[int]
    txn: dict

@dataclass
class Index:
    round: int
    intra: int
    path: list[int]


def save_payset(payset: list, round: int) -> int:
    """
    return the number of inserted rows, i.e. the no. of txns including inner txns
    """
    if not payset:
        return 0

    indexed_txns = [
        IndexedTxn([round, intra] + twp.path, twp.txn)
        for intra, txn in enumerate(payset)
        for twp in flatten_inner_txns(round, intra, txn)
    ]

    flattened_txns = [flatten_dict(asdict(txn)) for txn in indexed_txns]
    flattened_txns = [cut_prefix(txn, "txn_") for txn in flattened_txns]

    assert flattened_txns, "No txns to save. Should have already exited!!!"

    keys_set = set().union(*flattened_txns)
    schema = list(flattened_txns[0].keys())
    schema += [k for k in keys_set if k not in schema]
    print(f"""{schema=}
{keys_set=}""")

    df = pl.DataFrame(flattened_txns, schema=schema)
    for col in PROBLEM_COLS:
        if col in df.columns:
            prev = df[col]
            df.replace(col, prev.apply(str))
            
    global TXN_DF
    if TXN_DF is None:
        TXN_DF = df
    else:
        TXN_DF = pl.concat([TXN_DF, df], how="diagonal")
    
    TXN_DF.write_parquet(DELTA_LAKE_TXNS)
    return df.height


@dataclass
class TxnWithPath:
    txn: dict

    path: list[int]  # path == [] indicates root txn


def flatten_inner_txns(
    round: int,
    intra: int,
    stxn_w_ad: dict,
    path: list[int] | None = None,
    index: int = 0,
) -> list[TxnWithPath]:
    """
    Flatten a signed txn with application data (stxn_w_ad) into a list of txns
    Replace the recursive inner txns with their indices
    """
    inners = glom(stxn_w_ad, GPath(b"dt", b"itx"), default=[])
    path = cast(list[int], [] if path is None else path + [index])
    flattened = [twp := TxnWithPath(stxn_w_ad, path)]
    if not inners:
        return flattened

    inner_indices: list[Index] = []
    for index, inner in enumerate(inners):
        sub_inners = flatten_inner_txns(round, intra, inner, path, index)
        root_inner = sub_inners[0]
        inner_indices.append(Index(round, intra, root_inner.path))
        flattened.extend(sub_inners)
    twp.txn[b"dt"][b"itx"] = inner_indices

    return flattened


def debytify(v: object) -> object:
    if isinstance(v, bytes):
        return polars_decode(v)

    if isinstance(v, list):
        return [debytify(e) for e in v]

    if not isinstance(v, dict):
        return v

    return {debytify(k): debytify(w) for k, w in v.items()}


def flatten_dict(
    d: dict,
    parent_key: str = "",
    sep: str = "_",
):
    items = {}
    for k, v in d.items():
        if is_dataclass(v):
            v = asdict(v)

        if isinstance(k, bytes):
            try:
                k = k.decode("utf-8")
            except UnicodeDecodeError as e:
                raise ValueError(f"key [{k}] was not utf-8") from e
        k_str = str(k)

        if parent_key:
            k = f"{parent_key}{sep}{k}"

        if isinstance(v, dict) and k_str not in EXCLUDES:
            items.update(flatten_dict(v, k, sep=sep))
        else:
            items[k] = debytify(v)

    return items


def cut_prefix(d: dict, prefix: str):
    return {k[len(prefix) :] if k.startswith(prefix) else k: v for k, v in d.items()}


# def polars_save(flattened_txns: list[dict]) -> int:
#     assert flattened_txns, "No txns to save. Should have already exited!!!"

#     keys_set = set().union(*flattened_txns)
#     schema = list(flattened_txns[0].keys())
#     schema += [k for k in keys_set if k not in schema]

#     df = pl.DataFrame(flattened_txns, schema=schema)
#     return df.height


if __name__ == "__main__":
    print(f"Deleting {DELTA_LAKE_TXNS} if it exists")
    try:
        os.remove(DELTA_LAKE_TXNS)
    except FileNotFoundError:
        print("didn't actually delete it as non-existant...")

    app.run(host="0.0.0.0", port=1337)


"""
type TxnExtra struct {
	AssetCloseAmount uint64 `codec:"aca,omitempty"`
	RootIntra OptionalUint `codec:"root-intra,omitempty"`
	RootTxid string `codec:"root-txid,omitempty"`
}

type TxnRow struct {
	Round uint64
	RoundTime time.Time
	Intra int
	Txn *sdk.SignedTxnWithAD        <<<------- inner txns
	RootTxn *sdk.SignedTxnWithAD    <<<------- root txn for inner txns
	AssetID uint64
	Extra TxnExtra
	Error error
}

# Transaction Contains all fields common to all transactions and serves as an envelope
# to all transactions type. Represents both regular and inner transactions.
#
# Definition:
# data/transactions/signedtxn.go : SignedTxn
# data/transactions/transaction.go : Transaction
type Transaction struct {
	# ApplicationTransaction Fields for application transactions.
	#
	# Definition:
	# data/transactions/application.go : ApplicationCallTxnFields
	ApplicationTransaction *TransactionApplication `json:"application-transaction,omitempty"`

	# AssetConfigTransaction Fields for asset allocation, re-configuration, and destruction.
	#
	#
	# A zero value for asset-id indicates asset creation.
	# A zero value for the params indicates asset destruction.
	#
	# Definition:
	# data/transactions/asset.go : AssetConfigTxnFields
	AssetConfigTransaction *TransactionAssetConfig `json:"asset-config-transaction,omitempty"`

	# AssetFreezeTransaction Fields for an asset freeze transaction.
	#
	# Definition:
	# data/transactions/asset.go : AssetFreezeTxnFields
	AssetFreezeTransaction *TransactionAssetFreeze `json:"asset-freeze-transaction,omitempty"`

	# AssetTransferTransaction Fields for an asset transfer transaction.
	#
	# Definition:
	# data/transactions/asset.go : AssetTransferTxnFields
	AssetTransferTransaction *TransactionAssetTransfer `json:"asset-transfer-transaction,omitempty"`

	# AuthAddr \[sgnr\] this is included with signed transactions when the signing address does not equal the sender. The backend can use this to ensure that auth addr is equal to the accounts auth addr.
	AuthAddr *string `json:"auth-addr,omitempty"`

	# CloseRewards \[rc\] rewards applied to close-remainder-to account.
	CloseRewards *uint64 `json:"close-rewards,omitempty"`

	# ClosingAmount \[ca\] closing amount for transaction.
	ClosingAmount *uint64 `json:"closing-amount,omitempty"`

	# ConfirmedRound Round when the transaction was confirmed.
	ConfirmedRound *uint64 `json:"confirmed-round,omitempty"`

	# CreatedApplicationIndex Specifies an application index (ID) if an application was created with this transaction.
	CreatedApplicationIndex *uint64 `json:"created-application-index,omitempty"`

	# CreatedAssetIndex Specifies an asset index (ID) if an asset was created with this transaction.
	CreatedAssetIndex *uint64 `json:"created-asset-index,omitempty"`

	# Fee \[fee\] Transaction fee.
	Fee uint64 `json:"fee"`

	# FirstValid \[fv\] First valid round for this transaction.
	FirstValid uint64 `json:"first-valid"`

	# GenesisHash \[gh\] Hash of genesis block.
	GenesisHash *[]byte `json:"genesis-hash,omitempty"`

	# GenesisId \[gen\] genesis block ID.
	GenesisId *string `json:"genesis-id,omitempty"`

	# GlobalStateDelta Application state delta.
	GlobalStateDelta *StateDelta `json:"global-state-delta,omitempty"`

	# Group \[grp\] Base64 encoded byte array of a sha512/256 digest. When present indicates that this transaction is part of a transaction group and the value is the sha512/256 hash of the transactions in that group.
	Group *[]byte `json:"group,omitempty"`

	# Id Transaction ID
	Id *string `json:"id,omitempty"`

	# InnerTxns Inner transactions produced by application execution.
	InnerTxns *[]Transaction `json:"inner-txns,omitempty"`

	# IntraRoundOffset Offset into the round where this transaction was confirmed.
	IntraRoundOffset *uint64 `json:"intra-round-offset,omitempty"`

	# KeyregTransaction Fields for a keyreg transaction.
	#
	# Definition:
	# data/transactions/keyreg.go : KeyregTxnFields
	KeyregTransaction *TransactionKeyreg `json:"keyreg-transaction,omitempty"`

	# LastValid \[lv\] Last valid round for this transaction.
	LastValid uint64 `json:"last-valid"`

	# Lease \[lx\] Base64 encoded 32-byte array. Lease enforces mutual exclusion of transactions.  If this field is nonzero, then once the transaction is confirmed, it acquires the lease identified by the (Sender, Lease) pair of the transaction until the LastValid round passes.  While this transaction possesses the lease, no other transaction specifying this lease can be confirmed.
	Lease *[]byte `json:"lease,omitempty"`

	# LocalStateDelta \[ld\] Local state key/value changes for the application being executed by this transaction.
	LocalStateDelta *[]AccountStateDelta `json:"local-state-delta,omitempty"`

	# Logs \[lg\] Logs for the application being executed by this transaction.
	Logs *[][]byte `json:"logs,omitempty"`

	# Note \[note\] Free form data.
	Note *[]byte `json:"note,omitempty"`

	# PaymentTransaction Fields for a payment transaction.
	#
	# Definition:
	# data/transactions/payment.go : PaymentTxnFields
	PaymentTransaction *TransactionPayment `json:"payment-transaction,omitempty"`

	# ReceiverRewards \[rr\] rewards applied to receiver account.
	ReceiverRewards *uint64 `json:"receiver-rewards,omitempty"`

	# RekeyTo \[rekey\] when included in a valid transaction, the accounts auth addr will be updated with this value and future signatures must be signed with the key represented by this address.
	RekeyTo *string `json:"rekey-to,omitempty"`

	# RoundTime Time when the block this transaction is in was confirmed.
	RoundTime *uint64 `json:"round-time,omitempty"`

	# Sender \[snd\] Sender's address.
	Sender string `json:"sender"`

	# SenderRewards \[rs\] rewards applied to sender account.
	SenderRewards *uint64 `json:"sender-rewards,omitempty"`

	# Signature Validation signature associated with some data. Only one of the signatures should be provided.
	Signature *TransactionSignature `json:"signature,omitempty"`

	# StateProofTransaction Fields for a state proof transaction.
	#
	# Definition:
	# data/transactions/stateproof.go : StateProofTxnFields
	StateProofTransaction *TransactionStateProof `json:"state-proof-transaction,omitempty"`

	# TxType \[type\] Indicates what type of transaction this is. Different types have different fields.
	#
	# Valid types, and where their fields are stored:
	# * \[pay\] payment-transaction
	# * \[keyreg\] keyreg-transaction
	# * \[acfg\] asset-config-transaction
	# * \[axfer\] asset-transfer-transaction
	# * \[afrz\] asset-freeze-transaction
	# * \[appl\] application-transaction
	# * \[stpf\] state-proof-transaction
	TxType TransactionTxType `json:"tx-type"`
}

// TransactionApplication Fields for application transactions.
//
// Definition:
// data/transactions/application.go : ApplicationCallTxnFields
type TransactionApplication struct {
	// Accounts \[apat\] List of accounts in addition to the sender that may be accessed from the application's approval-program and clear-state-program.
	Accounts *[]string `json:"accounts,omitempty"`

	// ApplicationArgs \[apaa\] transaction specific arguments accessed from the application's approval-program and clear-state-program.
	ApplicationArgs *[]string `json:"application-args,omitempty"`

	// ApplicationId \[apid\] ID of the application being configured or empty if creating.
	ApplicationId uint64 `json:"application-id"`

	// ApprovalProgram \[apap\] Logic executed for every application transaction, except when on-completion is set to "clear". It can read and write global state for the application, as well as account-specific local state. Approval programs may reject the transaction.
	ApprovalProgram *[]byte `json:"approval-program,omitempty"`

	// ClearStateProgram \[apsu\] Logic executed for application transactions with on-completion set to "clear". It can read and write global state for the application, as well as account-specific local state. Clear state programs cannot reject the transaction.
	ClearStateProgram *[]byte `json:"clear-state-program,omitempty"`

	// ExtraProgramPages \[epp\] specifies the additional app program len requested in pages.
	ExtraProgramPages *uint64 `json:"extra-program-pages,omitempty"`

	// ForeignApps \[apfa\] Lists the applications in addition to the application-id whose global states may be accessed by this application's approval-program and clear-state-program. The access is read-only.
	ForeignApps *[]uint64 `json:"foreign-apps,omitempty"`

	// ForeignAssets \[apas\] lists the assets whose parameters may be accessed by this application's ApprovalProgram and ClearStateProgram. The access is read-only.
	ForeignAssets *[]uint64 `json:"foreign-assets,omitempty"`

	// GlobalStateSchema Represents a \[apls\] local-state or \[apgs\] global-state schema. These schemas determine how much storage may be used in a local-state or global-state for an application. The more space used, the larger minimum balance must be maintained in the account holding the data.
	GlobalStateSchema *StateSchema `json:"global-state-schema,omitempty"`

	// LocalStateSchema Represents a \[apls\] local-state or \[apgs\] global-state schema. These schemas determine how much storage may be used in a local-state or global-state for an application. The more space used, the larger minimum balance must be maintained in the account holding the data.
	LocalStateSchema *StateSchema `json:"local-state-schema,omitempty"`

	// OnCompletion \[apan\] defines the what additional actions occur with the transaction.
	//
	// Valid types:
	// * noop
	// * optin
	// * closeout
	// * clear
	// * update
	// * update
	// * delete
	OnCompletion OnCompletion `json:"on-completion"`
}

TODO: lots more.... for now let's just flatten
"""
