import base64

from flask import Flask, request, jsonify
import msgpack
import polars as pl

app = Flask(__name__)


def conform_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(conform_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


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


def save_txns(receive_payload: dict, content_type: str):
    assert (
        content_type == "application/msgpack"
    ), f"Unsupported content type {content_type}"
    return 1


if __name__ == "__main__":
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
	Txn *sdk.SignedTxnWithAD
	RootTxn *sdk.SignedTxnWithAD
	AssetID uint64
	Extra TxnExtra
	Error error
}
"""