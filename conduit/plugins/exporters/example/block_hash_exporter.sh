#!/bin/bash
set -eo pipefail

log() {
    local level=$1
    local message=$2
    local log_message
    log_message='{"__type": "exporter", "_name": "block_hash_exporter.sh", "level": "'$level'", "message": "'$message'", "time": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'"}'
    
    if [ "$level" == "ERROR" ]; then
        echo "$log_message" >&2
    else
        echo "$log_message"
    fi
}

# Function to initialize the script
init() {
    CONFIG_JSON=""
    GENESIS_JSON=""
    INIT_ROUND=0

    while [[ "$#" -gt 0 ]]; do
        case $1 in
            --config) CONFIG_JSON="$2"; shift ;;
            --genesis) GENESIS_JSON="$2"; shift ;;
            --init-round) INIT_ROUND="$2"; shift ;;
            *) 
                log "ERROR" "Unknown parameter passed: $1"
                exit 1 
                ;;
        esac
        shift
    done

    FILENAME=$(echo "$CONFIG_JSON" | jq -r '.output')

    echo "round,previous block hash" > "$FILENAME"
    log "INFO" "wrote header to $FILENAME"
}

daemon() {
    # Start reading from stdin
    while IFS= read -r line; do
        # Parse exportData command
        command=$(echo "$line" | jq -r '.conduitCommand')
        case $command in
            "config")
                echo "$CONFIG_JSON"
                ;;
            "close")
                log "INFO" 'Shutting down...'
                exit 0
                ;;
            "receive")
                BLOCK_DATA=$(echo "$line" | jq -r '.args.exportData')
                ROUND=$(echo "$BLOCK_DATA" | jq -r '.block.rnd')
                PREV=$(echo "$BLOCK_DATA" | jq -r '.block.prev')

                # Append round and previous block hash to CSV file
                echo "$ROUND,$PREV" >> "$FILENAME"

                # Log the write operation
                log "INFO" "wrote block data (round: $ROUND, previous block hash: $PREV) to $FILENAME"
                ;;
            *)
                log "ERROR" "Unknown command"
                exit 1
                ;;
        esac
    done
}

metadata() {
    echo '{"name": "Example Binary Exporter",  "description": "Very Simplistic", "deprecated": false, "sampleConfig": "{\"output\": \"block_hash.csv\"}}'
}


bhe() {
    case $1 in
        daemon) 
            shift
            init "$@"
            daemon
            ;;
        metadata)
            metadata
            ;;
        *)
            log "ERROR" "Unknown command: $1"
            exit 1 
            ;;
    esac
}

bhe "$@"
