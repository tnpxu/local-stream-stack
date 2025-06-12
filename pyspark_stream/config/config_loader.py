import yaml
import os

def load_config(config_path: str) -> dict:
    """
    Loads a YAML configuration file.

    Args:
        config_path: Path to the YAML configuration file.

    Returns:
        A dictionary containing the configuration.

    Raises:
        FileNotFoundError: If the config file is not found.
        yaml.YAMLError: If the config file is not valid YAML.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, 'r') as f:
        try:
            config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error parsing YAML file: {config_path}\n{e}")
    return config

# Example usage (optional, can be removed or commented out)
if __name__ == '__main__':
    try:
        # Assuming the script is run from the root directory
        # and 'configs/app_config.yaml' exists
        config = load_config('configs/app_config.yaml')
        print("Configuration loaded successfully:")
        print(config)

        # Example of accessing Kafka config
        kafka_config = config.get('kafka', {})
        print("\nKafka Bootstrap Servers:", kafka_config.get('bootstrap_servers'))
        print("Kafka Input Topic:", kafka_config.get('input_topic'))

        # Note on security:
        # Real username/password should be fetched from environment variables or a secure vault
        # For example:
        # kafka_username = os.getenv('KAFKA_USERNAME')
        # kafka_password = os.getenv('KAFKA_PASSWORD')
        # if kafka_username and kafka_password:
        #     print("\nKafka credentials are set via environment variables (not printed for security).")
        # else:
        #     print("\nKafka username/password not found in environment variables.")
        #     print("Ensure KAFKA_USERNAME and KAFKA_PASSWORD are set for production.")

    except FileNotFoundError as e:
        print(e)
    except yaml.YAMLError as e:
        print(e)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
