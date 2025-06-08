# 🚀 local-stream-stack 🚀

Spin up a powerful, local streaming development environment with **Apache Spark** and **Apache Kafka** on **Kubernetes (Docker Desktop)** in minutes.

This project provides a fully automated, operator-managed stack perfect for developing and testing stream processing applications without the headache of manual setup. It is the result of intensive, iterative debugging and represents a robust, stable configuration.

[![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?style=for-the-badge&logo=kubernetes&logoColor=white)]()
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)]()
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)]()
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)]()
[![Helm](https://img.shields.io/badge/Helm-0F1689?style=for-the-badge&logo=helm&logoColor=white)]()

---

## ✨ Final Architecture Features

* **Modern Kafka:** Runs a Zookeeper-less Kafka cluster in **KRaft mode**, eliminating an entire class of potential errors.
* **Operator-Managed:** Uses the **Spark Operator** and **Strimzi Kafka Operator** for robust, K8s-native application management.
* **NodePools Enabled:** Correctly configures Kafka using `KafkaNodePool` resources, as required by modern Strimzi versions for KRaft.
* **Fully Scripted:** A single `stack.sh` script handles the entire application lifecycle: deployment, status checks, teardown, and debugging.
* **Config-Driven:** All Kubernetes configurations are separated into clean, easy-to-read YAML files.
* **Secure & Simple Auth:** Comes pre-configured with a secure Kafka setup using SCRAM-SHA and a single, easy-to-manage `admin-user`.
* **Optimized for Docker Desktop:** Tailored to work seamlessly with the Kubernetes cluster provided by Docker Desktop, including dynamic IP discovery.

---

## 🛠️ Prerequisites

Make sure you have these tools installed on your local machine:

* [**Docker Desktop**](https://www.docker.com/products/docker-desktop/) - With the **Kubernetes** feature enabled in Settings.
* [**kubectl**](https://kubernetes.io/docs/tasks/tools/install-kubectl/) - The Kubernetes command-line tool.
* [**Helm**](https://helm.sh/docs/intro/install/) - The package manager for Kubernetes.
* [**kcat**](https://github.com/edenhill/kcat) (formerly kafkacat) - A command-line utility for testing Kafka.
    * *macOS:* `brew install kafkacat`
    * *Debian/Ubuntu:* `sudo apt-get install kafkacat`

---

## 📂 Final Directory Structure

Organize your project files as shown below for the scripts to work correctly.

```bash
├── configs/
│   ├── kafka/
│   │   ├── kafka-kraft-cluster.yaml  # Combined Kafka + NodePool definition
│   │   ├── kafka-users.yaml          # Defines ONLY the admin-user
│   │   └── strimzi-permissions.yaml  # Fixes K8s version detection
│   └── spark/
│       └── spark-service-account.yaml
├── stack.sh
├── sample_kafka_process.sh
└── README.md
```

---

## 🚀 Quick Start Guide

1.  **Enable Kubernetes in Docker Desktop:**
    Go to Docker Desktop `Settings > Kubernetes` and ensure `Enable Kubernetes` is ticked. Allocate sufficient resources (e.g., 4+ CPUs, 8GB+ Memory). If you run into problems, the **"Reset Kubernetes Cluster"** button is your best friend.

2.  **Set `kubectl` Context:**
    Make sure `kubectl` is pointing to `docker-desktop`. You can verify with `kubectl config current-context`.

3.  **Make Scripts Executable:**
    Open your terminal in the project directory.
    ```bash
    chmod +x stack.sh sample_kafka_process.sh
    ```

4.  **Initialize the Application Stack:**
    This command deploys all operators and the Kafka cluster. It may take several minutes as it waits for all components to become fully ready.
    ```bash
    ./stack.sh init
    ```

5.  **Check the Status:**
    Once initialized, use the `status` command to see if all pods are `Running`.
    ```bash
    ./stack.sh status
    ```

6.  **Get Kafka Credentials:**
    This retrieves the password for the `admin-user`.
    ```bash
    ./stack.sh creds
    ```

7. **Port Forwarding:**
    use port forwarding for expose Kafka
    ```bash
    kubectl port-forward service/my-kafka-cluster-kafka-external-bootstrap 9094:9094 -n kafka
    ```

8.  **Test Kafka:**
    Use the sample script to interact with your cluster.
    ```bash
    # List topics (will show internal topics at first)
    ./sample_kafka_process.sh list_topics

    # Create a topic and send some messages
    ./sample_kafka_process.sh create_topic my-first-topic
    ./sample_kafka_process.sh produce my-first-topic

    # In a second terminal, consume the messages
    ./sample_kafka_process.sh consume my-first-topic
    ```



> 🎉 **Congratulations!** Your local streaming stack is up and running.

---

## 🤖 Script Usage

### `stack.sh` - Environment Lifecycle

| Command                 | Description                                                              |
| ----------------------- | ------------------------------------------------------------------------ |
| `./stack.sh init`       | Deploys all stack applications to the cluster.                           |
| `./stack.sh status`     | Shows the current status of all stack components.                        |
| `./stack.sh creds`      | Retrieves the `admin-user` credentials                                   |
| `./stack.sh start`      | Scales up application pods to 1 (use after `stop`).                      |
| `./stack.sh stop`       | Scales down application pods to 0 to save resources.                     |
| `./stack.sh destroy`    | **Deletes all applications** from your Kubernetes cluster.               |
| `./stack.sh debug_kafka`| Gathers extensive logs and status to help debug the Kafka cluster.       |

### `sample_kafka_process.sh` - Kafka Testing

| Command                       | Description                                                     |
| ----------------------------- | --------------------------------------------------------------- |
| `./sample_kafka_process.sh list_topics` | Lists all brokers, topics, and consumer groups.                 |
| `./sample_kafka_process.sh create_topic <name>` | Creates a new topic.                                            |
| `./sample_kafka_process.sh produce <topic>` | Interactively produce messages to a topic.                      |
| `./sample_kafka_process.sh consume <topic>` | Consume and print all messages from a topic.                    |

---

## 🔍 Troubleshooting - Our Journey

This stack is now stable because we solved several complex issues. This section documents them for future reference.

| Error Message                                             | Cause & Explanation                                                                                                                             | How the Current Stack Solves It                                                                                                                                                              |
| --------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Detection of Kubernetes version failed`                  | The Strimzi Operator lacked RBAC permissions to ask the K8s API Server for its version.                                                           | The `init` script now applies `configs/kafka/strimzi-permissions.yaml` **before** installing the operator, guaranteeing the permission is always present.                                      |
| `KRaft can only be used with...KafkaNodePool resources`   | Strimzi requires that KRaft clusters define their nodes using the `KafkaNodePool` resource, not the old `spec.kafka.replicas` field.                | Our `configs/kafka/kafka-kraft-cluster.yaml` now correctly defines both the main `Kafka` resource and the required `KafkaNodePool` resource together.                                     |
| `cluster...does not have KafkaNodePool support enabled`   | The `Kafka` resource was missing the annotation `strimzi.io/node-pools: enabled` which is required to "activate" the `KafkaNodePool` functionality. | The `kafka-kraft-cluster.yaml` now includes **both** `strimzi.io/kraft: enabled` and `strimzi.io/node-pools: enabled` annotations in the `metadata` section.                               |
| `timed out waiting for...kafkausers`                      | The script was waiting for a `Ready` condition on the `KafkaUser` CR that Strimzi doesn't add.                                                      | The `init` and `creds` scripts now reliably wait for the **existence of the user's `Secret`**, which is the true sign that the user has been successfully created.                        |
| `Unresolved address` / `Couldn't bind` (DNS issues)         | These were symptoms of race conditions and networking complexity with the Zookeeper quorum.                                                       | The stack now runs in **KRaft mode**, completely eliminating Zookeeper and all associated startup/DNS problems. This is the most robust fix.                                             |
| `Host connection failures (Unresolved address, etc.)`| The networking between the host machine and the Docker Desktop Kubernetes environment can be unreliable. Connecting to localhost or a Node IP often fails.                                                         | The primary workflow now uses **kubectl port-forward.** This creates a direct, stable tunnel from your host to the Kafka service, bypassing all platform networking issues.                                  |

> **The Golden Rule of Debugging This Stack:** If `init` fails, run `./stack.sh destroy` and then `./stack.sh init` again. If it still fails, the `./stack.sh debug_kafka` command is your most powerful tool.
