# Example `bash` Exporter

## Dummy Run to Test Script

```sh
❯ ./block_hash_export.sh --config '{"output": "output.csv"}' --genesis '{"genesisBlock": "dummy genesis"}' --init-round 0

{"__type": "exporter", "_name": "block_hash_export.sh", "level": "INFO", "message": "wrote header to output.csv", "time": "2023-05-29T05:26:01Z"}

{"conduitCommand": "metadata"}
{"name": "Example Binary Exporter",  "description": "Very Simplistic", "deprecated": false, "sampleConfig": "{\"output\": \"block_hash.csv\"}}

{"conduitCommand": "config"}              
{"output": "output.csv"}

{"conduitCommand": "process", "args": {"input": {"block": {"rnd": 1, "prev": "QmFzZTY0IEVuY29kaW5nIDE="}}}}
{"__type": "exporter", "_name": "block_hash_export.sh", "level": "INFO", "message": "wrote block data (round: 1, previous block hash: QmFzZTY0IEVuY29kaW5nIDE=) to output.csv", "time": "2023-05-29T05:26:50Z"}

{"conduitCommand": "process", "args": {"input": {"block": {"rnd": 2, "prev": "QmFzZTY0IEVuY29kaW5nIDI="}, "payset": [], "delta": {}, "cert": {}}}}
{"__type": "exporter", "_name": "block_hash_export.sh", "level": "INFO", "message": "wrote block data (round: 2, previous block hash: QmFzZTY0IEVuY29kaW5nIDI=) to output.csv", "time": "2023-05-29T05:27:12Z"}

{"conduitCommand": "process", "args": {"input": {"block": {"rnd": 3, "prev": "QmFzZTY0IEVuY29kaW5nIDM="}}}}
{"conduitCommand": "process", "args": {"input": {"block": {"rnd": 4, "prev": "QmFzZTY0IEVuY29kaW5nIDQ="}}}}
{"conduitCommand": "process", "args": {"input": {"block": {"rnd": 5, "prev": "QmFzZTY0IEVuY29kaW5nIDU="}}}}

{"__type": "exporter", "_name": "block_hash_export.sh", "level": "INFO", "message": "wrote block data (round: 3, previous block hash: QmFzZTY0IEVuY29kaW5nIDM=) to output.csv", "time": "2023-05-29T05:27:24Z"}
{"__type": "exporter", "_name": "block_hash_export.sh", "level": "INFO", "message": "wrote block data (round: 4, previous block hash: QmFzZTY0IEVuY29kaW5nIDQ=) to output.csv", "time": "2023-05-29T05:27:24Z"}
{"__type": "exporter", "_name": "block_hash_export.sh", "level": "INFO", "message": "wrote block data (round: 5, previous block hash: QmFzZTY0IEVuY29kaW5nIDU=) to output.csv", "time": "2023-05-29T05:27:25Z"}

{"conduitCommand": "close"}
{"__type": "exporter", "_name": "block_hash_export.sh", "level": "INFO", "message": "Shutting down...", "time": "2023-05-29T05:27:39Z"}

❯ cat output.csv
round,previous block hash
1,QmFzZTY0IEVuY29kaW5nIDE=
2,QmFzZTY0IEVuY29kaW5nIDI=
3,QmFzZTY0IEVuY29kaW5nIDM=
4,QmFzZTY0IEVuY29kaW5nIDQ=
5,QmFzZTY0IEVuY29kaW5nIDU=
```
