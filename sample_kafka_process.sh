#!/usr/bin/env bash
set -e
set -o pipefail

# --- Configuration ---
readonly KAFKA_NAMESPACE="kafka"
readonly KAFKA_CLUSTER_NAME="my-kafka-cluster"
readonly CA_CERT_FILE="ca.crt"

# Color Codes
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[0;33m'
readonly RED='\033[0;31m'
readonly NC='\033[0m' # No Color

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
  if [ -n "$KCAT_BROKER" ]; then
    return
  fi

  info "Fetching Kafka connection details..."
  command -v kcat >/dev/null 2>&1 || error "kcat (kafkacat) is not installed. Please install it."
  if ! [ -f "$CA_CERT_FILE" ]; then
    error "'$CA_CERT_FILE' not found. Please run './stack.sh creds' first."
  fi

  # --- FIXED: Dynamically discover the Docker Desktop Node IP ---
  local DOCKER_NODE_IP
  DOCKER_NODE_IP=$(kubectl get node -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
  if [ -z "$DOCKER_NODE_IP" ]; then
      error "Could not discover the Docker Desktop Kubernetes Node IP address."
  fi

  local KAFKA_NODE_PORT
  KAFKA_NODE_PORT=$(kubectl get service -n "$KAFKA_NAMESPACE" "$KAFKA_CLUSTER_NAME-kafka-external-bootstrap" -o=jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null)
  if [ -z "$KAFKA_NODE_PORT" ]; then
    error "Could not retrieve Kafka NodePort. Is the stack running? Run './stack.sh status'."
  fi

  local admin_password
  admin_password=$(kubectl get secret -n "$KAFKA_NAMESPACE" admin-user -o jsonpath='{.data.password}' 2>/dev/null | base64 --decode)
  if [ -z "$admin_password" ]; then
    error "Could not retrieve password for 'admin-user'. Please run './stack.sh creds'."
  fi

  # Export variables for kcat to use
  export KCAT_BROKER="$DOCKER_NODE_IP:$KAFKA_NODE_PORT"
  export KCAT_USER="admin-user"
  export KCAT_PASSWORD="$admin_password"

  info "Kafka Broker endpoint set to: $KCAT_BROKER"
}

# Defines the standard kcat command with all necessary security flags.
run_kcat() {
  get_kafka_details
  kcat -b "$KCAT_BROKER" \
    -X security.protocol=SASL_SSL \
    -X sasl.mechanisms=SCRAM-SHA-512 \
    -X sasl.username="$KCAT_USER" \
    -X sasl.password="$KCAT_PASSWORD" \
    -X ssl.ca.location="$CA_CERT_FILE" \
    "$@"
}

# --- Core Kafka Operations ---
list_topics() {
  info "Listing brokers, topics, and consumer groups on the cluster..."
  run_kcat -L
}

create_topic() {
  local topic_name="$1"
  if [ -z "$topic_name" ]; then
    error "Topic name is required. Usage: $0 create_topic <topic-name>"
  fi
  info "Attempting to create topic '$topic_name' by producing a test message..."
  if echo "Topic creation message for $topic_name" | run_kcat -t "$topic_name" -P -c 1; then
    info "Topic '$topic_name' created successfully or already exists."
  else
    error "Failed to create topic '$topic_name'."
  fi
}

delete_topic() {
  local topic_name="$1"
  if [ -z "$topic_name" ]; then
    error "Topic name is required. Usage: $0 delete_topic <topic-name>"
  fi
  warn "Topic deletion with kcat is not straightforward."
  info "The recommended Strimzi way is to manage topics via 'KafkaTopic' Custom Resources."
  echo -e "${YELLOW}To delete '$topic_name', run: kubectl delete kafkatopic $topic_name -n $KAFKA_NAMESPACE ${NC}"
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
  echo "A simple tool to interact with the Kafka cluster using kcat."
  echo ""
  echo "--- Available Commands ---"
  echo "  list_topics             - Lists all brokers, topics, and consumer groups."
  echo "  create_topic <name>     - Creates a new topic (e.g., 'input-topic')."
  echo "  delete_topic <name>     - Provides instructions for deleting a topic via Strimzi."
  echo "  produce <topic>         - Interactively produce messages to a topic."
  echo "  consume <topic>         - Consume and print all messages from a topic."
}

case "$1" in
  list_topics|create_topic|delete_topic|produce|consume)
    "$@"
    ;;
  *)
    usage
    exit 1
    ;;
esac