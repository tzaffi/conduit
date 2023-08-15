from flask import Flask, request, jsonify

from algosdk import encoding


app = Flask(__name__)

@app.route('/receive', methods=['POST'])
def receive():
    content_type = request.headers.get('Content-Type')
    
    if content_type == "application/msgpack":
        print("handling msgpack")
        try:
            decoded_data = encoding.msgpack_decode(request.data)
        except Exception as e:
            return jsonify({"error": str(e)}), 400

    elif content_type == "application/json":
        print("handling json")
        try:
            decoded_data = request.json
        except Exception as e:
            return jsonify({"error": str(e)}), 400
    else:
        return jsonify({"error": "Unsupported content type"}), 400
    
    print(decoded_data)
    
    return jsonify({"success": True})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=1337)
