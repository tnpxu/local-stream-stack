#!/usr/bin/env bash
set -e # Exit immediately if a command exits with a non-zero status.
set -o pipefail # The return value of a pipeline is the status of the last command to exit with a non-zero status.

# --- Configuration ---
readonly KAFKA_NAMESPACE="kafka"
readonly SPARK_OPERATOR_NAMESPACE="spark-operator"
readonly SPARK_JOBS_NAMESPACE="default"

readonly KAFKA_CLUSTER_NAME="my-kafka-cluster"
readonly STRIMZI_HELM_RELEASE="strimzi-kafka-operator"
readonly SPARK_HELM_RELEASE="my-spark-operator"

# Config file paths
readonly KAFKA_CLUSTER_CONFIG="./configs/kafka/kafka-cluster.yaml"
readonly KAFKA_USERS_CONFIG="./configs/kafka/kafka-users.yaml"
readonly SPARK_SA_CONFIG="./configs/spark/spark-service-account.yaml"

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

check_deps() {
    info "Checking for dependencies (kubectl, helm, minikube)..."
    command -v kubectl >/dev/null 2>&1 || error "kubectl is not installed. Please install it."
    command -v helm >/dev/null 2>&1 || error "helm is not installed. Please install it."
    command -v minikube >/dev/null 2>&1 || error "minikube is not installed. Please install it."
    [ -f "$KAFKA_CLUSTER_CONFIG" ] || error "Kafka cluster config not found at $KAFKA_CLUSTER_CONFIG"
    [ -f "$KAFKA_USERS_CONFIG" ] || error "Kafka users config not found at $KAFKA_USERS_CONFIG"
    [ -f "$SPARK_SA_CONFIG" ] || error "Spark service account config not found at $SPARK_SA_CONFIG"
    info "All dependencies and config files are present."
}

check_k8s_context() {
    info "Checking Kubernetes context..."
    local current_context
    current_context=$(kubectl config current-context)
    if [ "$current_context" != "minikube" ]; then
        warn "Your kubectl context is set to '$current_context', not 'minikube'."
        warn "The script will proceed, but ensure this is the correct cluster."
        read -p "Do you want to continue? (y/n) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            error "Operation cancelled by user."
        fi
    else
        info "Kubernetes context is correctly set to 'minikube'."
    fi
    info "Checking Kubernetes cluster connectivity..."
    minikube status > /dev/null || error "Minikube does not seem to be running. Please run './stack.sh start-cluster'."
    kubectl cluster-info > /dev/null || error "Cannot connect to the Kubernetes cluster API."
}

# --- Core Functions ---

start_cluster() {
    info "Starting Minikube cluster with recommended resources for the stack..."
    if minikube status &>/dev/null; then
        warn "Minikube is already running."
    else
        minikube start --cpus=4 --memory=8192
    fi
    info "Minikube started successfully."
    info "If you plan to build local Docker images, run this in your terminal: eval \$(minikube -p minikube docker-env)"
}

stop_cluster() {
    info "Stopping the Minikube cluster..."
    minikube stop
}

init() {
    info "Initializing the full Spark and Kafka stack on Minikube..."
    check_deps
    check_k8s_context

    info "Creating namespaces..."
    kubectl get ns "$KAFKA_NAMESPACE" &>/dev/null || kubectl create namespace "$KAFKA_NAMESPACE"
    kubectl get ns "$SPARK_OPERATOR_NAMESPACE" &>/dev/null || kubectl create namespace "$SPARK_OPERATOR_NAMESPACE"

    info "Adding Helm repositories..."
    helm repo add strimzi https://strimzi.io/charts/
    helm repo add spark-operator https://kubeflow.github.io/spark-operator
    helm repo update

    info "Installing Strimzi Kafka Operator..."
    helm upgrade --install "$STRIMZI_HELM_RELEASE" strimzi/strimzi-kafka-operator \
        --namespace "$KAFKA_NAMESPACE" \
        --version 0.41.0

    info "Installing Spark Operator..."
    helm upgrade --install "$SPARK_HELM_RELEASE" spark-operator/spark-operator \
        --namespace "$SPARK_OPERATOR_NAMESPACE" \
        --set sparkJobNamespace="$SPARK_JOBS_NAMESPACE" \
        --set enableWebhook=true

    info "Deploying Kafka cluster from $KAFKA_CLUSTER_CONFIG..."
    kubectl apply -f "$KAFKA_CLUSTER_CONFIG" -n "$KAFKA_NAMESPACE"

    info "Creating Spark Service Account from $SPARK_SA_CONFIG..."
    kubectl apply -f "$SPARK_SA_CONFIG"

    info "Creating Kafka users from $KAFKA_USERS_CONFIG..."
    warn "Waiting 30 seconds for Kafka cluster to be initialized before creating users..."
    sleep 30
    kubectl apply -f "$KAFKA_USERS_CONFIG" -n "$KAFKA_NAMESPACE"

    info "\nStack initialization started. Use './stack.sh status' to check the progress."
    info "Use './stack.sh creds' to get credentials once the Kafka cluster is ready."
}

start() {
    info "Starting stack applications..."
    check_k8s_context
    info "Scaling up operators and Kafka cluster..."
    kubectl scale deployment -n "$SPARK_OPERATOR_NAMESPACE" --replicas=1 --all
    kubectl scale deployment -n "$KAFKA_NAMESPACE" --replicas=1 --all
    kubectl patch kafka "$KAFKA_CLUSTER_NAME" -n "$KAFKA_NAMESPACE" --type merge -p '{"spec":{"kafka":{"replicas":1},"zookeeper":{"replicas":1}}}'
    info "Stack applications start command issued. Monitor status with './stack.sh status'"
}

stop() {
    info "Stopping stack applications by scaling to 0 replicas..."
    check_k8s_context
    info "Scaling down operators and Kafka cluster..."
    kubectl scale deployment -n "$SPARK_OPERATOR_NAMESPACE" --replicas=0 --all
    kubectl scale deployment -n "$KAFKA_NAMESPACE" --replicas=0 --all
    kubectl patch kafka "$KAFKA_CLUSTER_NAME" -n "$KAFKA_NAMESPACE" --type merge -p '{"spec":{"kafka":{"replicas":0},"zookeeper":{"replicas":0}}}'
    info "Stack applications stopped. To restart, use './stack.sh start'"
}

status() {
    info "--- Stack Status ---"
    check_k8s_context

    info "\n[Minikube Status]"
    minikube status

    info "\n[Spark Operator Status ($SPARK_OPERATOR_NAMESPACE)]"
    kubectl get pods -n "$SPARK_OPERATOR_NAMESPACE"

    info "\n[Kafka/Strimzi Status ($KAFKA_NAMESPACE)]"
    kubectl get pods -n "$KAFKA_NAMESPACE"

    info "\n[Kafka Cluster CR Status]"
    # FIX: Wrapped the custom-columns query in single quotes
    kubectl get kafka "$KAFKA_CLUSTER_NAME" -n "$KAFKA_NAMESPACE" -o 'custom-columns=NAME:.metadata.name,STATE:.status.conditions[?(@.type=="Ready")].status' 2>/dev/null || warn "Kafka cluster status not available yet."

    info "\n[Active Spark Applications ($SPARK_JOBS_NAMESPACE)]"
    kubectl get sparkapplications -n "$SPARK_JOBS_NAMESPACE" 2>/dev/null || echo "No Spark applications found or CRD not ready."

    info "\n--- Access Info ---"
    local MINIKUBE_IP
    MINIKUBE_IP=$(minikube ip)
    local KAFKA_NODE_PORT
    KAFKA_NODE_PORT=$(kubectl get service -n "$KAFKA_NAMESPACE" "$KAFKA_CLUSTER_NAME-kafka-external-bootstrap" -o=jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null) || KAFKA_NODE_PORT="Not Ready"
    echo "Kafka External Bootstrap (SCRAM-SHA-512 + TLS): $MINIKUBE_IP:$KAFKA_NODE_PORT"
    echo "Kafka Internal Bootstrap (SCRAM-SHA-512): $KAFKA_CLUSTER_NAME-kafka-bootstrap.$KAFKA_NAMESPACE.svc.cluster.local:9092"
    echo "To access a Spark App UI, find its service with 'kubectl get svc -n $SPARK_JOBS_NAMESPACE' then run 'minikube service <service-name> -n $SPARK_JOBS_NAMESPACE'"
}

creds() {
    info "--- Kafka Credentials ---"
    check_k8s_context
    info "Retrieving credentials for Kafka users..."

    if ! kubectl get secret -n "$KAFKA_NAMESPACE" spark-user >/dev/null 2>&1; then
        error "Secret 'spark-user' not found. Is the stack fully initialized and ready?"
    fi

    local spark_password
    local admin_password
    spark_password=$(kubectl get secret -n "$KAFKA_NAMESPACE" spark-user -o jsonpath='{.data.password}' | base64 --decode)
    admin_password=$(kubectl get secret -n "$KAFKA_NAMESPACE" admin-user -o jsonpath='{.data.password}' | base64 --decode)

    echo -e "Username: ${YELLOW}spark-user${NC}"
    echo -e "Password: ${YELLOW}$spark_password${NC}"
    echo -e "\nUsername: ${YELLOW}admin-user${NC}"
    echo -e "Password: ${YELLOW}$admin_password${NC}"

    info "\nRetrieving Kafka Cluster CA Certificate for TLS..."
    kubectl get secret -n "$KAFKA_NAMESPACE" "$KAFKA_CLUSTER_NAME-cluster-ca-cert" -o jsonpath='{.data.ca\.crt}' | base64 --decode > ca.crt
    echo "CA certificate saved to ${GREEN}ca.crt${NC}. Use this for your external client's TLS/SSL configuration."
}

destroy() {
    warn "--- DESTROYING ALL STACK APPLICATIONS AND CONFIGS ---"
    check_k8s_context
    read -p "This will delete all operators and resources from Kubernetes. The Minikube VM will NOT be deleted. Continue? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        error "Destroy operation cancelled."
    fi

    info "Deleting resources from config files..."
    kubectl delete -f "$SPARK_SA_CONFIG" --ignore-not-found=true
    kubectl delete -f "$KAFKA_USERS_CONFIG" -n "$KAFKA_NAMESPACE" --ignore-not-found=true
    kubectl delete -f "$KAFKA_CLUSTER_CONFIG" -n "$KAFKA_NAMESPACE" --ignore-not-found=true

    info "Deleting Helm releases..."
    helm uninstall "$SPARK_HELM_RELEASE" -n "$SPARK_OPERATOR_NAMESPACE" --ignore-errors || warn "Could not uninstall Spark Operator."
    helm uninstall "$STRIMZI_HELM_RELEASE" -n "$KAFKA_NAMESPACE" --ignore-errors || warn "Could not uninstall Strimzi."

    info "Deleting Namespaces..."
    kubectl delete namespace "$KAFKA_NAMESPACE" --ignore-not-found=true
    kubectl delete namespace "$SPARK_OPERATOR_NAMESPACE" --ignore-not-found=true
    
    info "Deleting local CA certificate file..."
    rm -f ca.crt

    info "Stack applications destroyed successfully. To delete the cluster itself, run 'minikube delete'."
}

usage() {
    echo "Usage: $0 {start-cluster|stop-cluster|init|start|stop|status|creds|destroy}"
    echo "--- Cluster Management ---"
    echo "  start-cluster - Starts the Minikube virtual machine."
    echo "  stop-cluster  - Stops the Minikube virtual machine."
    echo ""
    echo "--- Application Management ---"
    echo "  init      - Deploys all stack applications (Operators, Kafka) to the running cluster."
    echo "  start     - Starts/scales up stack applications."
    echo "  stop      - Stops/scales down stack applications."
    echo "  status    - Shows the current status of all stack components."
    echo "  creds     - Retrieves Kafka credentials and CA certificate."
    echo "  destroy   - Deletes all stack applications from the cluster."
}

# --- Main Command Router ---
case "$1" in
    start-cluster|stop-cluster|init|start|stop|status|creds|destroy)
        "$1"
        ;;
    *)
        usage
        exit 1
        ;;
esac