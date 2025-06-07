#!/usr/bin/env bash
set -e
set -o pipefail

# --- Configuration ---
readonly KAFKA_NAMESPACE="kafka"
readonly KAFKA_BROKER="127.0.0.1:9094"

# Color Codes
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[0;33m'
readonly RED='\033[0;31m'
readonly NC='\033[0m'

# --- Helper Functions ---
info() {
  echo -e "${GREEN}[INFO] ${*}${NC}"
}
warn() {
  echo -e "${YELLOW}[WARN] ${*}${NC}"
}
error() {
  echo -e "${RED}[ERROR] ${*}${NC}"
  exit 1
}

# Fetches Kafka connection details and credentials.
get_kafka_details() {
  if [ -n "$KCAT_USER" ]; then
    return
  fi

  info "Fetching Kafka connection details..."
  command -v kcat >/dev/null 2>&1 || error "kcat (kafkacat) is not installed. Please install it."

  local admin_password
  admin_password=$(kubectl get secret -n "$KAFKA_NAMESPACE" admin-user -o jsonpath='{.data.password}' 2>/dev/null | base64 --decode)
  if [ -z "$admin_password" ]; then
    error "Could not retrieve password for 'admin-user'. Please run './stack.sh creds'."
  fi

  export KCAT_USER="admin-user"
  export KCAT_PASSWORD="$admin_password"
}

# Defines the standard kcat command with all necessary security flags.
run_kcat() {
  get_kafka_details
  # --- MODIFIED: Using SASL_PLAINTEXT and removed all SSL flags ---
  kcat -b "$KAFKA_BROKER" \
    -X security.protocol=SASL_PLAINTEXT \
    -X sasl.mechanisms=SCRAM-SHA-512 \
    -X sasl.username="$KCAT_USER" \
    -X sasl.password="$KCAT_PASSWORD" \
    "$@"
}

# --- Core Kafka Operations ---
list_topics() {
  info "Listing brokers, topics, and consumer groups on cluster at $KAFKA_BROKER..."
  run_kcat -L
}

create_topic() {
  local topic_name="$1"
  if [ -z "$topic_name" ]; then
    error "Topic name is required. Usage: $0 create_topic <topic-name>"
  fi
  info "Attempting to create topic '$topic_name'..."
  if echo "Topic creation message for $topic_name" | run_kcat -t "$topic_name" -P -c 1; then
    info "Topic '$topic_name' created successfully or already exists."
  else
    error "Failed to create topic '$topic_name'."
  fi
}

produce() {
  local topic_name="$1"
  if [ -z "$topic_name" ]; then
    error "Topic name is required. Usage: $0 produce <topic-name>"
  fi
  info "Starting interactive producer for topic '${YELLOW}$topic_name${NC}'."
  info "Type a message and press Enter to send. Press ${YELLOW}Ctrl+D${NC} to exit."
  run_kcat -t "$topic_name" -P
}

consume() {
  local topic_name="$1"
  if [ -z "$topic_name" ]; then
    error "Topic name is required. Usage: $0 consume <topic-name>"
  fi
  info "Starting consumer for topic '${YELLOW}$topic_name${NC}'."
  info "Listening for messages... Press ${YELLOW}Ctrl+C${NC} to exit."
  run_kcat -t "$topic_name" -C
}

# --- Main Command Router ---
usage() {
  echo "Usage: $0 {command} [arguments]"
  echo ""
  echo "A simple tool to interact with the Kafka cluster using kcat (SASL_PLAINTEXT)."
  echo ""
  echo "--- Available Commands ---"
  echo "  list_topics             - Lists all brokers, topics, and consumer groups."
  echo "  create_topic <name>     - Creates a new topic."
  echo "  produce <topic>         - Interactively produce messages to a topic."
  echo "  consume <topic>         - Consume and print all messages from a topic."
}

case "$1" in
  list_topics|create_topic|produce|consume)
    "$@"
    ;;
  *)
    usage
    exit 1
    ;;
esac