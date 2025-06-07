#!/usr/bin/env bash
set -e # Exit immediately if a command exits with a non-zero status.
set -o pipefail # The return value of a pipeline is the status of the last command to exit with a non-zero status.

# --- Configuration ---
readonly KAFKA_NAMESPACE="kafka"
readonly SPARK_OPERATOR_NAMESPACE="spark-operator"
readonly SPARK_JOBS_NAMESPACE="default"
readonly KAFKA_CLUSTER_NAME="my-kafka-cluster"
readonly KAFKA_NODEPOOL_NAME="controller-broker"
readonly STRIMZI_HELM_RELEASE="strimzi-kafka-operator"
readonly STRIMZI_VERSION="0.42.0"
readonly SPARK_HELM_RELEASE="my-spark-operator"

# Paths to local configuration files
readonly KAFKA_KRAFT_CONFIG="./configs/kafka/kafka-kraft-cluster.yaml"
readonly KAFKA_USERS_CONFIG="./configs/kafka/kafka-users.yaml"
readonly SPARK_SA_CONFIG="./configs/spark/spark-service-account.yaml"
readonly STRIMZI_PERMISSIONS_CONFIG="./configs/kafka/strimzi-permissions.yaml"

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

check_deps() {
  info "Checking for dependencies..."
  command -v kubectl >/dev/null 2>&1 || error "kubectl is not installed."
  command -v helm >/dev/null 2>&1 || error "helm is not installed."
  command -v curl >/dev/null 2>&1 || error "curl is not installed."

  if ! [ -f "$STRIMZI_PERMISSIONS_CONFIG" ] || \
     ! [ -f "$KAFKA_KRAFT_CONFIG" ] || \
     ! [ -f "$KAFKA_USERS_CONFIG" ] || \
     ! [ -f "$SPARK_SA_CONFIG" ]; then
    error "One or more required config files are missing."
  fi

  info "All dependencies and config files are present."
}

check_k8s_context() {
  info "Checking Kubernetes context..."
  local ctx
  ctx=$(kubectl config current-context)

  if [ "$ctx" != "docker-desktop" ]; then
    warn "Context is '$ctx', not 'docker-desktop'."
    read -p "Continue? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
      error "Operation cancelled."
    fi
  fi

  if ! kubectl cluster-info >/dev/null; then
    error "Cannot connect to Kubernetes API. Is Docker Desktop running with Kubernetes enabled?"
  fi
}


# --- Core Functions ---
init() {
  info "Initializing stack..."
  check_deps
  check_k8s_context

  info "Creating namespaces..."
  if ! kubectl get ns "$KAFKA_NAMESPACE" &>/dev/null; then
    kubectl create namespace "$KAFKA_NAMESPACE"
  fi
  if ! kubectl get ns "$SPARK_OPERATOR_NAMESPACE" &>/dev/null; then
    kubectl create namespace "$SPARK_OPERATOR_NAMESPACE"
  fi

  info "Applying Strimzi permissions..."
  kubectl apply -f "$STRIMZI_PERMISSIONS_CONFIG"

  info "Adding Helm repos..."
  helm repo add strimzi https://strimzi.io/charts/
  helm repo add spark-operator https://kubeflow.github.io/spark-operator
  helm repo update

  info "Installing Strimzi Operator v$STRIMZI_VERSION..."
  helm upgrade --install "$STRIMZI_HELM_RELEASE" strimzi/strimzi-kafka-operator \
    --namespace "$KAFKA_NAMESPACE" \
    --version "$STRIMZI_VERSION" \
    --wait

  info "Strimzi operator is running."

  info "Installing Spark Operator..."
  helm upgrade --install "$SPARK_HELM_RELEASE" spark-operator/spark-operator \
    --namespace "$SPARK_OPERATOR_NAMESPACE" \
    --set sparkJobNamespace="$SPARK_JOBS_NAMESPACE" \
    --set enableWebhook=true \
    --wait
  info "Spark operator is running."

  info "Deploying Kafka cluster and NodePool in KRaft mode..."
  kubectl apply -f "$KAFKA_KRAFT_CONFIG" -n "$KAFKA_NAMESPACE"

  info "Waiting for Kafka cluster to be ready..."
  if ! kubectl wait kafka "$KAFKA_CLUSTER_NAME" -n "$KAFKA_NAMESPACE" --for=condition=Ready --timeout=300s; then
    error "Kafka cluster not ready. Use './stack.sh debug_kafka'."
  fi

  info "Creating other resources..."
  kubectl apply -f "$SPARK_SA_CONFIG"
  kubectl apply -f "$KAFKA_USERS_CONFIG" -n "$KAFKA_NAMESPACE"

  # --- MODIFIED: Waiting for the 'admin-user' secret to exist ---
  info "Waiting for 'admin-user' credentials secret to be generated..."
  if ! kubectl wait --for=existence "secret/admin-user" -n "$KAFKA_NAMESPACE" --timeout=120s; then
    error "Secret for KafkaUser 'admin-user' was not created in time. Use './stack.sh debug_kafka' to investigate."
  fi

  info "\n✅ Stack initialization complete."
}

destroy() {
  warn "--- DESTROYING STACK ---"
  check_k8s_context
  read -p "This will delete all operators and resources from Kubernetes. Continue? (y/n) " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    error "Destroy cancelled."
  fi

  info "Deleting resources..."
  kubectl delete --ignore-not-found=true -f "$SPARK_SA_CONFIG"
  kubectl delete --ignore-not-found=true -f "$KAFKA_USERS_CONFIG" -n "$KAFKA_NAMESPACE"
  kubectl delete --ignore-not-found=true -f "$KAFKA_KRAFT_CONFIG" -n "$KAFKA_NAMESPACE"
  kubectl delete --ignore-not-found=true -f "$STRIMZI_PERMISSIONS_CONFIG"

  info "Deleting Helm releases..."
  if ! helm uninstall "$SPARK_HELM_RELEASE" -n "$SPARK_OPERATOR_NAMESPACE" --ignore-errors; then
    warn "Could not uninstall Spark Operator."
  fi
  if ! helm uninstall "$STRIMZI_HELM_RELEASE" -n "$KAFKA_NAMESPACE" --ignore-errors; then
    warn "Could not uninstall Strimzi."
  fi

  info "Deleting Namespaces..."
  kubectl delete namespace "$KAFKA_NAMESPACE" --ignore-not-found=true
  kubectl delete namespace "$SPARK_OPERATOR_NAMESPACE" --ignore-not-found=true

  info "Deleting local CA file..."
  rm -f ca.crt
  info "Stack destroyed."
}

start() {
  info "Starting applications..."
  check_k8s_context
  kubectl scale deployment -n "$SPARK_OPERATOR_NAMESPACE" --replicas=1 --all
  kubectl scale deployment -n "$KAFKA_NAMESPACE" --replicas=1 --all
  kubectl patch kafkanodepool "$KAFKA_NODEPOOL_NAME" -n "$KAFKA_NAMESPACE" \
    --type merge -p '{"spec":{"replicas":1}}'
  info "Start command issued."
}

stop() {
  info "Stopping applications..."
  check_k8s_context
  kubectl scale deployment -n "$SPARK_OPERATOR_NAMESPACE" --replicas=0 --all
  kubectl scale deployment -n "$KAFKA_NAMESPACE" --replicas=0 --all
  kubectl patch kafkanodepool "$KAFKA_NODEPOOL_NAME" -n "$KAFKA_NAMESPACE" \
    --type merge -p '{"spec":{"replicas":0}}'
  info "Stop command issued."
}

status() {
  info "--- Stack Status ---"
  check_k8s_context
  info "\n[Kubernetes Node Status]"; kubectl get nodes -o wide
  info "\n[Spark Operator Status ($SPARK_OPERATOR_NAMESPACE)]"; kubectl get pods -n "$SPARK_OPERATOR_NAMESPACE"
  info "\n[Kafka/Strimzi Status ($KAFKA_NAMESPACE)]"; kubectl get pods -n "$KAFKA_NAMESPACE"
  info "\n[Kafka Cluster CR Status]"; if ! kubectl get kafka "$KAFKA_CLUSTER_NAME" -n "$KAFKA_NAMESPACE" -o 'custom-columns=NAME:.metadata.name,STATE:.status.conditions[?(@.type=="Ready")].status' 2>/dev/null; then warn "Kafka cluster status not available."; fi
  info "\n[Active Spark Applications ($SPARK_JOBS_NAMESPACE)]"; if ! kubectl get sparkapplications -n "$SPARK_JOBS_NAMESPACE" 2>/dev/null; then echo "No Spark applications found."; fi

  info "\n--- Access Info ---"
  local DOCKER_NODE_IP
  DOCKER_NODE_IP=$(kubectl get node -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
  local KAFKA_NODE_PORT
  KAFKA_NODE_PORT=$(kubectl get service -n "$KAFKA_NAMESPACE" "$KAFKA_CLUSTER_NAME-kafka-external-bootstrap" -o=jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null) || KAFKA_NODE_PORT="Not Ready"

  echo "Kafka External Bootstrap: $DOCKER_NODE_IP:$KAFKA_NODE_PORT"
  echo "Kafka Internal Bootstrap: $KAFKA_CLUSTER_NAME-kafka-bootstrap.$KAFKA_NAMESPACE.svc.cluster.local:9092"
  echo "To access Spark App UI, use 'kubectl port-forward svc/<service-name> ...'"
}

creds() {
  info "--- Kafka Credentials ---"
  check_k8s_context
  
  # --- MODIFIED: Waiting for and getting credentials for 'admin-user' only ---
  info "Waiting for 'admin-user' secret..."
  if ! kubectl wait --for=existence "secret/admin-user" -n "$KAFKA_NAMESPACE" --timeout=60s; then
    error "Secret 'admin-user' not found."
  fi

  local admin_password
  admin_password=$(kubectl get secret -n "$KAFKA_NAMESPACE" admin-user -o jsonpath='{.data.password}' | base64 --decode)

  echo -e "\nUsername: ${YELLOW}admin-user${NC}\nPassword: ${YELLOW}$admin_password${NC}"

  info "\nRetrieving Kafka Cluster CA Certificate..."
  kubectl get secret -n "$KAFKA_NAMESPACE" "$KAFKA_CLUSTER_NAME-cluster-ca-cert" -o jsonpath='{.data.ca\.crt}' | base64 --decode > ca.crt
  echo "CA certificate saved to ${GREEN}ca.crt${NC}."
}

debug_kafka() {
  info "--- Running In-Depth Kafka Debug ---"
  check_k8s_context
  local operator_pod
  operator_pod=$(kubectl get pods -n "$KAFKA_NAMESPACE" -l strimzi.io/kind=cluster-operator -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

  info "\n[1] Logs from Strimzi Operator Pod (includes User Operator logs):"
  if [ -n "$operator_pod" ]; then kubectl logs "$operator_pod" -n "$KAFKA_NAMESPACE" --tail=200; else warn "Could not get logs, Strimzi operator pod not found."; fi

  info "\n[2] Describing 'KafkaUser' resource 'admin-user':"
  if ! kubectl describe kafkauser admin-user -n "$KAFKA_NAMESPACE"; then warn "Could not describe KafkaUser 'admin-user'."; fi

  info "\n[3] Listing Secrets in '$KAFKA_NAMESPACE' namespace:"
  kubectl get secrets -n "$KAFKA_NAMESPACE"

  info "\n[4] Describing the Strimzi Operator Pod itself:"
  if [ -n "$operator_pod" ]; then kubectl describe pod "$operator_pod" -n "$KAFKA_NAMESPACE"; fi
  
  info "\n--- End of Kafka Debug ---"
}

usage() {
  echo "Usage: $0 {command}"
  echo ""
  echo "Manages the local Spark/Kafka development stack on Docker Desktop."
  echo ""
  echo "--- Available Commands ---"
  echo "  init          Deploys all stack applications."
  echo "  start         Starts/scales up stack applications."
  echo "  stop          Stops/scales down stack applications."
  echo "  status        Shows the current status of all stack components."
  echo "  creds         Retrieves Kafka credentials and the CA certificate."
  echo "  destroy       Deletes all stack applications from the cluster."
  echo "  debug_kafka   Gathers detailed logs and status to debug the Kafka cluster."
}


# --- Main Command Router ---
case "$1" in
  init|start|stop|destroy|status|creds|debug_kafka)
    "$1"
    ;;
  *)
    usage
    exit 1
    ;;
esac