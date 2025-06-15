# 🚀 local-stream-stack 🚀

Spin up a powerful, local streaming development environment with **Apache Spark** and **Apache Kafka** on **Kubernetes (Docker Desktop)** in minutes.

This project provides a fully automated, operator-managed stack perfect for developing and testing stream processing applications. It is the result of intensive, iterative debugging and represents a robust, stable configuration for local development.

[![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?style=for-the-badge&logo=kubernetes&logoColor=white)]()
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)]()
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)]()
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)]()
[![Helm](https://img.shields.io/badge/Helm-0F1689?style=for-the-badge&logo=helm&logoColor=white)]()

---

## ✨ Final Architecture Features

* **Modern Kafka:** Runs a Zookeeper-less Kafka cluster in **KRaft mode**.
* **Operator-Managed:** Uses the **Spark Operator** and **Strimzi Kafka Operator**.
* **NodePools Enabled:** Correctly configures Kafka using `KafkaNodePool` resources.
* **Fully Scripted:** A single `stack.sh` script handles the entire infrastructure lifecycle.
* **Config-Driven:** All Kubernetes configurations are separated into clean YAML files.
* **Secure & Simple Auth:** Pre-configured with a secure Kafka setup and a single `admin-user`.
* **Reliable Connectivity:** Emphasizes using `kubectl port-forward` for stable host access.
* **Example Application:** Includes a sample PySpark streaming job to get you started immediately.

---

## 📂 Final Directory Structure

```bash
├── apps
│   └── pyspark-stream-job
│       ├── sample_stream_job.py
│       └── stream_job.py
├── configs
│   ├── kafka
│   │   ├── kafka-kraft-cluster.yaml
│   │   ├── kafka-users.yaml
│   │   └── strimzi-permissions.yaml
│   ├── spark
│   │   └── spark-service-account.yaml
│   ├── app_config.yaml
│   ├── pipeline_config.yaml
│   └── stream_job_pipeline_config.yaml
├── pyspark_stream
│   ├── connectors
│   │   └── kafka_connector.py
│   ├── config
│   │   └── config_loader.py
│   ├── models
│   │   └── schema_manager.py
│   ├── pipeline
│   │   └── streaming_pipeline.py
│   ├── processors
│   │   ├── base_processor.py
│   │   └── custom_processors.py
│   ├── state
│   │   └── offset_manager.py
│   └── utils
│       └── logger.py
├── README.md
├── sample_kafka_process.sh
├── setup_local_dev.sh
└── stack.sh
```

---

## 🚀 The Local Dev Workflow

### Part 1: Start the Infrastructure

1.  **Start Docker Desktop:** Ensure Kubernetes is enabled and has sufficient resources (4+ CPUs, 8GB+ RAM).
2.  **Set `kubectl` Context:** Make sure your context is set to `docker-desktop`.
3.  **Initialize the Stack:** Run our script to deploy Kafka and the Spark Operator.
    ```bash
    ./stack.sh init
    ```
4.  **Open the Connection Tunnel:** For interacting with Kafka from your local machine, you must open the `port-forward` tunnel in a **dedicated terminal window** and leave it running.
    ```bash
    kubectl port-forward service/my-kafka-cluster-kafka-external-bootstrap 9094:9094 -n kafka
    ```
5.  **Expose k8s proxy:** for expose k8s service in local run this command
    ```bash
    kubectl proxy
    ```
    

### Part 2: Run the Spark Streaming Job

See the `README.md` file inside the `apps/pyspark-stream-job` directory for instructions on how to build, run, and test the Spark application. *(This assumes you would create a specific README for that app).*

```bash
Your Machine (IDE)          |      Kubernetes Cluster
-----------------------------|--------------------------------
 [ Python Driver Process ] <-|--+--- [ Spark Executor Pod ]
       | (localhost:9092)    |  |          |
       |                     |  |          | (Connects to Kafka Service)
       v                     |  |          v
 [ kubectl port-forward ] <--+--+--- [ Kafka Bootstrap Service ]
```

---

## 🤖 Script Usage

### `stack.sh` - Environment Lifecycle

| Command                 | Description                                                              |
| ----------------------- | ------------------------------------------------------------------------ |
| `./stack.sh init`       | Deploys all stack applications to the cluster.                           |
| `./stack.sh status`     | Shows the current status of all stack components.                        |
| `./stack.sh creds`      | Retrieves the `admin-user` credentials.                                  |
| `./stack.sh start`      | Scales up application pods to 1 (use after `stop`).                      |
| `./stack.sh stop`       | Scales down application pods to 0 to save resources.                     |
| `./stack.sh destroy`    | **Deletes all applications** from your Kubernetes cluster.               |
| `./stack.sh debug_kafka`| Gathers extensive logs and status to help debug the Kafka cluster.      |

### `sample_kafka_process.sh` - Kafka Utilities

This script is a powerful tool for managing and testing your Kafka topics.

#### Topic Management
| Command                       | Description                                                     |
| ----------------------------- | --------------------------------------------------------------- |
| `./sample_kafka_process.sh list_topics` | Lists all brokers, topics, and consumer groups.                 |
| `./sample_kafka_process.sh create_topic <name>` | Creates a standard topic (old data is deleted by time/size).    |
| `./sample_kafka_process.sh delete_topic <name>` | Deletes a topic managed by Strimzi.                             |
| `./sample_kafka_process.sh reset_topic <name>`  | Deletes and immediately recreates a topic. Perfect for testing. |

#### Log Compaction & Key-Based Deletion
| Command                                 | Description                                                                |
| --------------------------------------- | -------------------------------------------------------------------------- |
| `./sample_kafka_process.sh create_compacted_topic <name>` | Creates a topic where only the last message for each key is kept. |
| `./sample_kafka_process.sh produce_with_key <topic> <key> <value>` | Produces a message with a specific key. |
| `./sample_kafka_process.sh delete_by_key <topic> <key>` | "Deletes" a message by producing a tombstone for its key. |

#### Producing & Consuming Data
| Command                                      | Description                                                          |
| -------------------------------------------- | -------------------------------------------------------------------- |
| `./sample_kafka_process.sh produce <topic>`        | Interactively produce keyless messages to a topic.                 |
| `./sample_kafka_process.sh consume <topic>`        | Consume all messages from the beginning and wait for new ones.     |
| `./sample_kafka_process.sh consume_from_offset <topic> <offset>` | Consume messages starting from a specific offset and wait. |

#### Draining Messages (Read and Exit)
| Command                                      | Description                                                          |
| -------------------------------------------- | -------------------------------------------------------------------- |
| `./sample_kafka_process.sh drain <topic>`          | Reads all messages from the beginning of a topic and exits.        |
| `./sample_kafka_process.sh drain_from_offset <topic> <offset>` | Reads messages from a specific offset to the end and exits. |

---

##🧪 Example: Testing Log Compaction

Here is how you can use the new utilities to see how log compaction works.

1.  **Create a compacted topic:**
    ```bash
    ./sample_kafka_process.sh create_compacted_topic user_profiles
    ```

2.  **Add a profile for user `user123`:**
    ```bash
    ./sample_kafka_process.sh produce_with_key user_profiles user123 '{"name": "Alice", "city": "New York"}'
    ```

3.  **Update the profile for the same user:**
    ```bash
    ./sample_kafka_process.sh produce_with_key user_profiles user123 '{"name": "Alice", "city": "London", "status": "active"}'
    ```

4.  **"Delete" the profile for user `user123`:** This sends a "tombstone" message.
    ```bash
    ./sample_kafka_process.sh delete_by_key user_profiles user123
    ```
> **Note:** The actual removal of older messages for `user123` is not immediate. Kafka's log cleaner process runs in the background. After some time, only the final tombstone message for `user123` will remain, until it too is cleaned up.

---

## 🔍 Troubleshooting Log

This stack is now stable because we solved several complex issues. This section documents them for future reference.

| Error We Encountered                                      | The Final, Working Solution                                                                                                                                                              |
| --------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Detection of Kubernetes version failed`                  | The `init` script now applies `configs/kafka/strimzi-permissions.yaml` **before** installing the operator, guaranteeing it has the required RBAC permissions.                                 |
| `KRaft requires KafkaNodePools` & `NodePool support not enabled` | Our `configs/kafka/kafka-kraft-cluster.yaml` now correctly defines the `Kafka` resource with the `strimzi.io/node-pools: enabled` annotation, and a corresponding `KafkaNodePool` resource in the same file. |
| `timed out waiting for...kafkausers`                      | The `init` script now reliably waits for the **existence of the user's `Secret`** instead of a `Ready` condition that doesn't exist on the `KafkaUser` CRD.                                     |
| Host Connection Failures (`Unresolved address`, `Connection timed out`, etc.) | The `kafka-kraft-cluster.yaml` now uses an `advertisedHost` and `advertisedPort` override in the external listener, forcing all clients to connect back through the reliable `kubectl port-forward` tunnel. |
| getSubject is supported only if a security manager is allowed | recommended use sdkman to change JDK version to 21 |
|getSubject is supported only if a security manager is allowed (Java Gateway Error) | The host machine's modern JDK was incompatible. Solution: Abandoned running Spark on the host. The final workflow uses a local Spark installation (spark-local) managed by findspark to ensure a compatible, self-contained Java environment.|
|Could not find a 'KafkaClient' entry in the JAAS configuration (Kafka Auth Error) | Setting the JAAS config globally on the SparkSession was not working. Solution: Moved the kafka.sasl.jaas.config property from the global Spark config into a direct .option() on both the batch verification (.read) and the streaming (.readStream) operations. This forces the Kafka client to use the correct credentials for each specific connection.|
|namespaces "spark-apps" not found (Kubernetes Permissions Error) |The Spark driver's ServiceAccount lacked the permissions to create resources like ConfigMaps in its namespace. Solution: Updated configs/spark/spark-service-account.yaml to include a Role with permissions for pods, services, and configmaps, and a RoleBinding to grant that role to the spark service account. |
| | |
| | |


---
## 🧹 Teardown

To completely clean up your local environment, stop the `port-forward` command (`Ctrl+C`) and run:
```bash
./stack.sh destroy