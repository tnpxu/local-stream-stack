#!/usr/bin/env bash
set -e
set -o pipefail

# --- Configuration ---
readonly KAFKA_NAMESPACE="kafka"
readonly KAFKA_CLUSTER_NAME="my-kafka-cluster"
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
  
  info "Creating KafkaTopic resource '$topic_name' to be managed by Strimzi..."
  
  # Using a "here document" to create the YAML and pipe it to kubectl
  cat <<EOF | kubectl apply -n "$KAFKA_NAMESPACE" -f -
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: $topic_name
  labels:
    strimzi.io/cluster: $KAFKA_CLUSTER_NAME
spec:
  partitions: 1
  replicas: 1
EOF
  
  info "Waiting for topic '$topic_name' to become ready..."
  if ! kubectl wait "kafkatopic/$topic_name" -n "$KAFKA_NAMESPACE" --for=condition=Ready --timeout=60s; then
      error "Topic '$topic_name' did not become ready in time."
  fi
  info "Topic '$topic_name' created and ready."
}

delete_topic() {
  local topic_name="$1"
  if [ -z "$topic_name" ]; then
    error "Topic name is required. Usage: $0 delete_topic <topic-name>"
  fi
  
  warn "This will delete the Kafka topic '$topic_name' from the cluster."
  read -p "Are you sure? (y/n) " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
      info "Operation cancelled."
      exit 0
  fi

  info "Deleting KafkaTopic resource '$topic_name'..."
  if ! kubectl delete kafkatopic "$topic_name" -n "$KAFKA_NAMESPACE"; then
    error "Failed to delete topic '$topic_name'. Does it exist?"
  fi
  info "Topic '$topic_name' deleted successfully."
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
  info "Starting consumer for topic '${YELLOW}$topic_name${NC}' from the beginning."
  info "Listening for messages... Press ${YELLOW}Ctrl+C${NC} to exit."
  run_kcat -t "$topic_name" -C -o beginning
}

consume_from_offset() {
  local topic_name="$1"
  local offset="$2"
  if [ -z "$topic_name" ] || [ -z "$offset" ]; then
    error "Topic name and offset are required. Usage: $0 consume_from_offset <topic-name> <offset>"
  fi
  info "Starting consumer for topic '${YELLOW}$topic_name${NC}' from offset ${YELLOW}$offset${NC}."
  info "Listening for messages... Press ${YELLOW}Ctrl+C${NC} to exit."
  run_kcat -t "$topic_name" -C -o "$offset"
}

drain() {
  local topic_name="$1"
  if [ -z "$topic_name" ]; then
    error "Topic name is required. Usage: $0 drain <topic-name>"
  fi
  info "Draining all messages from topic '${YELLOW}$topic_name${NC}' from the beginning..."
  # -e flag exits kcat when the last message is reached
  run_kcat -t "$topic_name" -C -o beginning -e
  info "Drain complete."
}

drain_from_offset() {
  local topic_name="$1"
  local offset="$2"
  if [ -z "$topic_name" ] || [ -z "$offset" ]; then
    error "Topic name and offset are required. Usage: $0 drain_from_offset <topic-name> <offset>"
  fi
  info "Draining messages from topic '${YELLOW}$topic_name${NC}' starting at offset ${YELLOW}$offset${NC}..."
  run_kcat -t "$topic_name" -C -o "$offset" -e
  info "Drain complete."
}

# --- Main Command Router ---
usage() {
  echo "Usage: $0 {command} [arguments]"
  echo ""
  echo "A tool to interact with the Kafka cluster using kcat."
  echo ""
  echo "--- Topic Management ---"
  echo "  list_topics             - Lists all brokers, topics, and consumer groups."
  echo "  create_topic <name>     - Creates a new topic managed by Strimzi."
  echo "  delete_topic <name>     - Deletes a topic managed by Strimzi."
  echo ""
  echo "--- Producing and Consuming ---"
  echo "  produce <topic>         - Interactively produce messages to a topic."
  echo "  consume <topic>         - Consume all messages and wait for new ones."
  echo "  consume_from_offset <topic> <offset> - Consume messages starting from a specific offset."
  echo ""
  echo "--- Draining Messages (Consume and Exit) ---"
  echo "  drain <topic>           - Drains all messages from the beginning of a topic and exits."
  echo "  drain_from_offset <topic> <offset> - Drains messages from a specific offset and exits."
}

case "$1" in
  list_topics|create_topic|delete_topic|produce|consume|consume_from_offset|drain|drain_from_offset)
    "$@"
    ;;
  *)
    usage
    exit 1
    ;;
esac