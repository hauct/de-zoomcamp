import os

# Convert client.properties file into a dict 
def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf


# Get the path of client.properties
CURRENT_FILE_PATH = os.path.abspath(__file__)
SRC_DIR = os.path.dirname(CURRENT_FILE_PATH)
BASE_DIR = os.path.dirname(SRC_DIR)

CLIENT_PROPERTIES_FILE_PATH = os.path.join(BASE_DIR, 'resources', 'client.properties')

# =====================================================================

# Change to a dict contains infomation
CONFLUENT_CLOUD_CONFIG = read_ccloud_config(CLIENT_PROPERTIES_FILE_PATH)

"""
{
    'bootstrap.servers': 'pkc-ldvr1.asia-southeast1.gcp.confluent.cloud:9092', 
    'security.protocol': 'SASL_SSL', 'sasl.mechanisms': 'PLAIN', 
    'sasl.username': 'NOFMFAPAP67ZK5NB', 
    'sasl.password': 'rYCv7Feu/5GyaGsFsHmmziip/nsxPadGoIGxQVHHQE5zA1/UYzAGqvWcPGiY6Cxa', 
    'session.timeout.ms': '45000'
    }
"""

# Kafka topic
TOPIC = 'nyc_rides'
