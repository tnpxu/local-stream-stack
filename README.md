# 🚀 local-stream-stack 🚀

Spin up a powerful, local streaming development environment with **Apache Spark** and **Apache Kafka** on **Kubernetes (Minikube)** in minutes.

This project provides a fully automated, operator-managed stack perfect for developing and testing stream processing applications without the headache of manual setup.

[![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?style=for-the-badge&logo=kubernetes&logoColor=white)]()
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)]()
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)]()
[![Helm](https://img.shields.io/badge/Helm-0F1689?style=for-the-badge&logo=helm&logoColor=white)]()
[![Shell Script](https://img.shields.io/badge/Shell_Script-121011?style=for-the-badge&logo=gnu-bash&logoColor=white)]()

---

## ✨ Features

* **Operator-Managed:** Uses the **Spark Operator** and **Strimzi Kafka Operator** for robust, K8s-native application management.
* **Fully Scripted:** A single `stack.sh` script handles the entire lifecycle: cluster startup, application deployment, teardown, and more.
* **Config-Driven:** All Kubernetes configurations are separated into clean, easy-to-read YAML files. No hardcoding.
* **Authentication Ready:** Comes pre-configured with a secure Kafka setup using SCRAM-SHA and TLS.
* **Ready for Testing:** Includes a `sample_kafka_process.sh` script to instantly produce/consume messages and test your Kafka topics.
* **Local Focus:** Optimized for local development on **Minikube**.

---

## 🛠️ Prerequisites

Make sure you have these tools installed on your local machine:

* [**Minikube**](https://minikube.sigs.k8s.io/docs/start/) - For running the local Kubernetes cluster.
* [**kubectl**](https://kubernetes.io/docs/tasks/tools/install-kubectl/) - The Kubernetes command-line tool.
* [**Helm**](https://helm.sh/docs/intro/install/) - The package manager for Kubernetes.
* [**kafkacat**](https://github.com/edenhill/kcat) - A command-line utility for testing Kafka.
    * *macOS:* `brew install kafkacat`
    * *Debian/Ubuntu:* `sudo apt-get install kafkacat`

---

## 📂 Directory Structure

Organize your project files as shown below for the scripts to work correctly.

├── configs/
│   ├── kafka/
│   │   ├── kafka-cluster.yaml
│   │   └── kafka-users.yaml
│   └── spark/
│       └── spark-service-account.yaml
├── stack.sh
├── sample_kafka_process.sh
└── README.md

---

## 🚀 Quick Start Guide

1.  **Clone the Repository (or set up the files):**
    Ensure all the files from this project are in a single directory.

2.  **Make Scripts Executable:**
    Open your terminal and grant execute permissions to the scripts.
    ```bash
    chmod +x stack.sh sample_kafka_process.sh
    ```

3.  **Start the Kubernetes Cluster:**
    This command starts your Minikube VM with the recommended resources.
    ```bash
    ./stack.sh start-cluster
    ```

4.  **Initialize the Application Stack:**
    Deploy Spark Operator, Strimzi, and the Kafka cluster onto Minikube. This may take several minutes.
    ```bash
    ./stack.sh init
    ```

5.  **Check the Status:**
    Once initialized, use the `status` command to see if all pods are running.
    ```bash
    ./stack.sh status
    ```

6.  **Get Kafka Credentials:**
    This command retrieves Kafka user passwords and generates the `ca.crt` file needed by your clients.
    ```bash
    ./stack.sh creds
    ```

7.  **Test Kafka:**
    Use the sample script to create a topic, produce messages, and consume them.
    ```bash
    # In one terminal, create a topic and produce to it
    ./sample_kafka_process.sh create-topic my-test-topic
    ./sample_kafka_process.sh produce my-test-topic

    # In a second terminal, consume from it
    ./sample_kafka_process.sh consume my-test-topic
    ```

> 🎉 **You're all set!** Your local streaming stack is up and running. You can now start submitting Spark jobs that connect to this Kafka cluster.

---

## 🤖 Script Usage

All operations are managed via the two main scripts.

### `stack.sh` - Environment Lifecycle

This script manages the entire infrastructure.

| Command                     | Description                                                                    |
| --------------------------- | ------------------------------------------------------------------------------ |
| `./stack.sh start-cluster`  | Starts the Minikube virtual machine.                                           |
| `./stack.sh init`           | Deploys all applications (Operators, Kafka) onto the running cluster.          |
| `./stack.sh status`         | Shows the current status of all stack components.                              |
| `./stack.sh creds`          | Retrieves Kafka credentials and the `ca.crt` file for client connections.      |
| `./stack.sh start`          | Scales up application pods (use after `stop`).                                 |
| `./stack.sh stop`           | Scales down application pods to 0 to save resources (preserves config).        |
| `./stack.sh stop-cluster`   | Stops the Minikube virtual machine.                                            |
| `./stack.sh destroy`        | **Deletes all applications** from Kubernetes (but not the Minikube VM itself). |

### `sample_kafka_process.sh` - Kafka Testing

This script is for interacting with your Kafka topics.

| Command                           | Description                                                                    |
| --------------------------------- | ------------------------------------------------------------------------------ |
| `./sample_kafka_process.sh list-topics` | Lists all topics and brokers in the cluster.                                   |
| `./sample_kafka_process.sh create-topic <name>` | Creates a new topic (e.g., `input-topic`).                                       |
| `./sample_kafka_process.sh produce <topic>` | Starts an interactive producer to send messages to a topic.                    |
| `./sample_kafka_process.sh consume <topic>` | Starts a consumer to listen for and print messages from a topic.               |
| `./sample_kafka_process.sh delete-topic <name>` | Shows instructions on how to properly delete a topic using Strimzi.            |

---

## 🧹 Teardown

To completely clean up your local environment:

1.  **Destroy the Applications:**
    This removes all Kubernetes resources deployed by the `init` command.
    ```bash
    ./stack.sh destroy
    ```

2.  **Delete the Minikube Cluster:**
    This will completely delete the Minikube virtual machine and all its data.
    ```bash
    minikube delete
    ```
