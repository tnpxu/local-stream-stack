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
readonly NC='\033[0m'

# --- Helper Functions ---
info() {
    echo -e "${GREEN}[INFO] ${*}${NC}"
}

error() {
    echo -e "${RED}[ERROR] ${*}${NC}"
    exit 1
}

# Function to get connection details automatically
get_kafka_details() {
    # This function is memoized to avoid repeated kubectl/minikube calls
    if [ -n "$KAFKA_BROKER" ]; then
        return
    fi

    info "Fetching Kafka connection details..."
    command -v kafkacat >/dev/null 2>&1 || error "kafkacat is not installed. Please install it."
    [ -f "$CA_CERT_FILE" ] || error "'$CA_CERT_FILE' not found. Please run './stack.sh creds' first."

    local MINIKUBE_IP
    local KAFKA_NODE_PORT
    MINIKUBE_IP=$(minikube ip)
    KAFKA_NODE_PORT=$(kubectl get service -n "$KAFKA_NAMESPACE" "$KAFKA_CLUSTER_NAME-kafka-external-bootstrap" -o=jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null)

    if [ -z "$KAFKA_NODE_PORT" ]; then
        error "Could not retrieve Kafka NodePort. Is the stack running? Run './stack.sh status'."
    fi

    local admin_password
    admin_password=$(kubectl get secret -n "$KAFKA_NAMESPACE" admin-user -o jsonpath='{.data.password}' | base64 --decode)
    if [ -z "$admin_password" ]; then
        error "Could not retrieve password for 'admin-user'. Please run './stack.sh creds'."
    fi

    # Export variables for kafkacat to use
    export KAFKA_BROKER="$MINIKUBE_IP:$KAFKA_NODE_PORT"
    export KAFKA_USER="admin-user"
    export KAFKA_PASSWORD="$admin_password"

    info "Kafka Broker: $KAFKA_BROKER"
}

# --- Core Kafka Operations ---

list_topics() {
    info "Listing topics on cluster..."
    get_kafka_details
    kafkacat -b "$KAFKA_BROKER" \
        -X security.protocol=SASL_SSL \
        -X sasl.mechanisms=SCRAM-SHA-512 \
        -X sasl.username="$KAFKA_USER" \
        -X sasl.password="$KAFKA_PASSWORD" \
        -X ssl.ca.location="$CA_CERT_FILE" \
        -L
}

create_topic() {
    local topic_name="$1"
    if [ -z "$topic_name" ]; then
        error "Topic name is required. Usage: $0 create-topic <topic-name>"
    fi
    info "Creating topic '$topic_name'... (Note: This relies on broker's auto.create.topics.enable=true or admin rights)"
    # The 'admin-user' has superUser rights, which allows creating topics by producing to them.
    # This is a simple way to "create" a topic without using the kafka-topics.sh script.
    echo "Test message to create topic" | produce_to_topic "$topic_name"
    info "Topic '$topic_name' created or already exists."
}

delete_topic() {
    local topic_name="$1"
    if [ -z "$topic_name" ]; then
        error "Topic name is required. Usage: $0 delete-topic <topic-name>"
    fi
    warn "DELETE is not directly supported by kafkacat. This requires the kafka-topics.sh script and exec'ing into the Kafka pod."
    warn "To delete a topic, you can use the Strimzi KafkaTopic CRD. Create a file like this:"
    echo -e "${YELLOW}
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: $topic_name
  labels:
    strimzi.io/cluster: $KAFKA_CLUSTER_NAME
# By removing the spec, Strimzi will delete the topic.
# Then run 'kubectl delete -f your-file.yaml -n $KAFKA_NAMESPACE'
"
}

produce_to_topic() {
    local topic_name="$1"
    if [ -z "$topic_name" ]; then
        error "Topic name is required. Usage: $0 produce <topic-name>"
    fi
    get_kafka_details

    info "Starting interactive producer for topic '${YELLOW}$topic_name${NC}'."
    info "Type a message and press Enter to send. Press ${YELLOW}Ctrl+D${NC} to exit."

    kafkacat -b "$KAFKA_BROKER" \
        -X security.protocol=SASL_SSL \
        -X sasl.mechanisms=SCRAM-SHA-512 \
        -X sasl.username="$KAFKA_USER" \
        -X sasl.password="$KAFKA_PASSWORD" \
        -X ssl.ca.location="$CA_CERT_FILE" \
        -t "$topic_name" \
        -P # Producer mode
}

consume_from_topic() {
    local topic_name="$1"
    if [ -z "$topic_name" ]; then
        error "Topic name is required. Usage: $0 consume <topic-name>"
    fi
    get_kafka_details

    info "Starting consumer for topic '${YELLOW}$topic_name${NC}'."
    info "Listening for messages... Press ${YELLOW}Ctrl+C${NC} to exit."

    kafkacat -b "$KAFKA_BROKER" \
        -X security.protocol=SASL_SSL \
        -X sasl.mechanisms=SCRAM-SHA-512 \
        -X sasl.username="$KAFKA_USER" \
        -X sasl.password="$KAFKA_PASSWORD" \
        -X ssl.ca.location="$CA_CERT_FILE" \
        -t "$topic_name" \
        -C # Consumer mode
}

usage() {
    echo "Usage: $0 {list-topics|create-topic|produce|consume|delete-topic}"
    echo "  list-topics             - Lists all topics, brokers, and consumer groups in the cluster."
    echo "  create-topic <name>     - Creates a new topic (e.g., input-topic)."
    echo "  produce <topic>         - Interactively produce messages to a topic."
    echo "  consume <topic>         - Consume and print messages from a topic."
    echo "  delete-topic <name>     - Shows instructions on how to delete a topic using Strimzi CRDs."
}

# --- Main Command Router ---
case "$1" in
    list-topics)
        list_topics
        ;;
    create-topic)
        create_topic "$2"
        ;;
    produce)
        produce_to_topic "$2"
        ;;
    consume)
        consume_from_topic "$2"
        ;;
    delete-topic)
        delete_topic "$2"
        ;;
    *)
        usage
        exit 1
        ;;
esac