import logging 
import logging.config
import yaml 
import json 
import datetime
import os 
import connexion
from connexion import NoContent 
import requests
import random
import uuid
import pykafka 
from pykafka import KafkaClient

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())
    
with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

# client = KafkaClient(hosts='<kafka-server>:<kafka-port>')
# f"{app_config['events']['hostname']}:{app_config['events']['port']}"
# topic = client.topics[str.encode(app_config['events']['topic'])]
# producer = topic.get_sync_producer() 

def add_book_hold(body): 
    trace_id = str(uuid.uuid4())
    message = f"Receievd event book hold request with trace_id: {trace_id}"
    logger.info(message)

    event_object = {
        "trace_id": trace_id,
        **body
    }

    # response = requests.post(
    #     app_config['eventstore1']['url'],
    #     json=body,
    #     headers={"Content-Type": "application/json"}
    # )
    client = KafkaClient(hosts=f"{app_config['events']['hostname']}:{app_config['events']['port']}")
    topic = client.topics[str.encode(app_config['events']['topic'])]
    producer = topic.get_sync_producer() 
    
    msg = { "type": "book",
            "datetime" :
                datetime.datetime.now().strftime(
                    "%Y-%m-%dT%H:%M:%S"),
            "payload": event_object }
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))
    logger.info("Sending the book hold request to Kafka")

    # logger.info(f"Returned event movie hold request with response {trace_id} with status {response.status_code}")
    return NoContent, 201


def add_movie_hold(body):
    trace_id = str(uuid.uuid4())
    message = f"Receievd event movie hold request with trace_id: {trace_id}"
    logger.info(message)


    event_object = {
        "trace_id": trace_id,
        **body
    }

    # response = requests.post(
    #     app_config['eventstore2']['url'],
    #     json=body,
    #     headers={"Content-Type": "application/json"}
    # )
    client = KafkaClient(hosts=f"{app_config['events']['hostname']}:{app_config['events']['port']}")
    topic = client.topics[str.encode(app_config['events']['topic'])]
    producer = topic.get_sync_producer() 
    
    msg = { "type": "movie",
            "datetime" :
                datetime.datetime.now().strftime(
                    "%Y-%m-%dT%H:%M:%S"),
            "payload": event_object }
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))
    logger.info("Sending the book hold request to Kafka")
    # logger.info(f"Returned event movie hold request with response {trace_id} with status {response.status_code}")
    return NoContent, 201

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml",
            strict_validation=True,
            validate_responses=True)

if __name__ == "__main__":
    app.run(port=8080)

