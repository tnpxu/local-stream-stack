#!/bin/bash

# This script downloads and sets up a local Spark environment for development.
# This avoids system-wide installation and potential Java/Python conflicts.

set -e

SPARK_VERSION="3.5.0"
HADOOP_VERSION="3"
SPARK_ARCHIVE="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
SPARK_DOWNLOAD_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_ARCHIVE}"
INSTALL_DIR="spark-local"

# Check if Spark is already installed
if [ -d "$INSTALL_DIR" ]; then
    echo "Local Spark installation already found in ./$INSTALL_DIR/"
    echo "Skipping download."
    echo ""
else
    echo "--- Downloading Spark ${SPARK_VERSION} ---"
    # Download Spark
    wget -q --show-progress -O "$SPARK_ARCHIVE" "$SPARK_DOWNLOAD_URL"

    echo "--- Unpacking Spark ---"
    # Create installation directory
    mkdir -p "$INSTALL_DIR"
    # Unpack the archive
    tar -xzf "$SPARK_ARCHIVE" -C "$INSTALL_DIR" --strip-components=1

    # Clean up the downloaded archive
    rm "$SPARK_ARCHIVE"
    echo "--- Spark has been installed in ./$INSTALL_DIR ---"
    echo ""
fi

# Provide instructions to the user
echo "--- Configuration required ---"
echo "To use the local Spark installation, please set the following environment variables"
echo "in your shell configuration file (e.g., ~/.bashrc, ~/.zshrc) or export them"
echo "in your current terminal session:"
echo ""
echo "export SPARK_HOME=$(pwd)/$INSTALL_DIR"
echo "export JAVA_HOME=\$(/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home)" # For macOS, adjust for your OS if needed
echo "export PATH=\$SPARK_HOME/bin:\$PATH"
echo ""
echo "After setting these, you can run the local development script."

