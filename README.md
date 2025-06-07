# 🚀 local-stream-stack 🚀

Spin up a powerful, local streaming development environment with **Apache Spark** and **Apache Kafka** on **Kubernetes (Docker Desktop)** in minutes.

This project provides a fully automated, operator-managed stack perfect for developing and testing stream processing applications without the headache of manual setup.

[![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?style=for-the-badge&logo=kubernetes&logoColor=white)]()
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)]()
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)]()
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)]()
[![Helm](https://img.shields.io/badge/Helm-0F1689?style=for-the-badge&logo=helm&logoColor=white)]()

---

## ✨ Features

* **Operator-Managed:** Uses the **Spark Operator** and **Strimzi Kafka Operator** for robust, K8s-native application management.
* **Fully Scripted:** A single `stack.sh` script handles the entire application lifecycle: deployment, status checks, teardown, and more.
* **Config-Driven:** All Kubernetes configurations are separated into clean, easy-to-read YAML files.
* **Authentication Ready:** Comes pre-configured with a secure Kafka setup using SCRAM-SHA and TLS.
* **Ready for Testing:** Includes a `sample_kafka_process.sh` script to instantly produce/consume messages and test your Kafka topics.
* **Optimized for Docker Desktop:** Tailored to work seamlessly with the Kubernetes cluster provided by Docker Desktop.

---

## 🛠️ Prerequisites

Make sure you have these tools installed on your local machine:

* [**Docker Desktop**](https://www.docker.com/products/docker-desktop/) - With the **Kubernetes** feature enabled in Settings.
* [**kubectl**](https://kubernetes.io/docs/tasks/tools/install-kubectl/) - The Kubernetes command-line tool.
* [**Helm**](https://helm.sh/docs/intro/install/) - The package manager for Kubernetes.
* [**kafkacat**](https://github.com/edenhill/kcat) - A command-line utility for testing Kafka.
    * *macOS:* `brew install kafkacat`
    * *Debian/Ubuntu:* `sudo apt-get install kafkacat`

---

## 📂 Directory Structure

Organize your project files as shown below for the scripts to work correctly.

```bash
├── configs/
│   ├── kafka/
│   │   ├── kafka-cluster.yaml
│   │   ├── kafka-users.yaml
│   │   └── strimzi-permissions.yaml
│   └── spark/
│       └── spark-service-account.yaml
├── stack.sh
├── sample_kafka_process.sh
└── README.md
```

---

## 🚀 Quick Start Guide

1.  **Enable Kubernetes in Docker Desktop:**
    Go to Docker Desktop `Settings > Kubernetes` and ensure the checkbox for `Enable Kubernetes` is ticked. Also, ensure you allocate sufficient resources (e.g., 4+ CPUs, 8GB+ Memory).

2.  **Set `kubectl` Context:**
    Make sure `kubectl` is pointing to Docker Desktop. Click the Docker whale icon in your system tray and ensure the `kubectl` context is set to `docker-desktop`.

3.  **Make Scripts Executable:**
    Open your terminal in the project directory.
    ```bash
    chmod +x stack.sh sample_kafka_process.sh
    ```

4.  **Initialize the Application Stack:**
    This command deploys Spark Operator, Strimzi, and the Kafka cluster. It may take several minutes as it waits for all components to become fully ready.
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

### `stack.sh` - Environment Lifecycle

This script manages the entire infrastructure on Docker Desktop.

| Command                     | Description                                                                    |
| --------------------------- | ------------------------------------------------------------------------------ |
| `./stack.sh init`           | Deploys all applications (Operators, Kafka) to the cluster.                    |
| `./stack.sh status`         | Shows the current status of all stack components.                              |
| `./stack.sh creds`          | Retrieves Kafka credentials and the `ca.crt` file for client connections.      |
| `./stack.sh start`          | Scales up application pods to 1 (use after `stop`).                            |
| `./stack.sh stop`           | Scales down application pods to 0 to save resources (preserves config).        |
| `./stack.sh destroy`        | **Deletes all applications** from your Kubernetes cluster.                     |
| `./stack.sh debug-kafka`    | Gathers extensive logs and status to help debug the Kafka cluster.              |

### `sample_kafka_process.sh` - Kafka Testing

This script is for interacting with your Kafka topics.

| Command                           | Description                                                                    |
| --------------------------------- | ------------------------------------------------------------------------------ |
| `./sample_kafka_process.sh list-topics` | Lists all topics and brokers in the cluster.                                   |
| `./sample_kafka_process.sh create-topic <name>` | Creates a new topic (e.g., `input-topic`).                                       |
| `./sample_kafka_process.sh produce <topic>` | Starts an interactive producer to send messages to a topic.                    |
| `./sample_kafka_process.sh consume <topic>` | Starts a consumer to listen for and print messages from a topic.               |

---

## 🔍 Troubleshooting

We hit a few common issues during setup. The current `stack.sh` script is designed to prevent them, but here's a reference for what they were and how they were solved.

| Error Message                              | Cause                                                                       | How the Script Solves It                                                                                                |
| ------------------------------------------ | --------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `Secret 'spark-user' not found`            | A timing issue where we tried to get credentials before the operator created them. | The `init` script now has robust `kubectl wait` commands that pause execution until the Kafka cluster and users are truly ready. |
| `Detection of Kubernetes version failed`   | The Strimzi operator lacked RBAC permissions to check the K8s API version.    | The `init` script now applies `configs/kafka/strimzi-permissions.yaml` **before** installing the operator, guaranteeing the permission is present. |

> **First step for any new issue:** If your Kafka cluster fails to start, always run `./stack.sh debug-kafka` first. It provides detailed logs and status descriptions that are crucial for identifying the problem.

---

## 🧹 Teardown

To completely clean up your local environment:

1.  **Destroy the Applications:**
    This removes all Kubernetes resources deployed by the `init` command.
    ```bash
    ./stack.sh destroy
    ```

2.  **Disable or Reset Kubernetes:**
    Go to `Docker Desktop > Settings > Kubernetes` and either uncheck `Enable Kubernetes` or click `Reset Kubernetes Cluster` for a full reset.