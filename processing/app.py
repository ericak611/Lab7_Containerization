import requests
import logging
import logging.config
import yaml
import json
import datetime
import os
import connexion
from connexion import NoContent
import random
import uuid
from apscheduler.schedulers.background import BackgroundScheduler

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')


def populate_stats():
    # Log an INFO message
    logger.info("Start periodic processing.")

    hold_requests = {
        'num_bh_requests': 0,
        'num_mh_requests': 0,
        'max_bh_availability': 0,
        'max_mh_availability': 0,
        'last_updated': '2010-10-10 11:17:50.225086'
    }

    if os.path.exists(app_config['datastore']['filename']):
        with open(app_config['datastore']['filename'], 'r') as file:
            hold_requests = json.load(file)

    book_response = requests.get(
        app_config['eventstore']['url'] + "/book",
        params={"timestamp": hold_requests['last_updated']}
    )

    movie_response = requests.get(
        app_config['eventstore']['url'] + "/movie",
        params={"timestamp": hold_requests['last_updated']}
    )

    # Log based on status code
    if book_response.status_code == 200:
        new_book_requests = book_response.json()
        num_book_requests = len(new_book_requests) - hold_requests['num_bh_requests']
        if num_book_requests > 0:
            hold_requests['num_bh_requests'] += num_book_requests

        logger.info(f"Received {num_book_requests} events from /book")
    else:
        logger.error("Failed to get events from /book")

    # Log based on status code
    if movie_response.status_code == 200:
        new_movie_requests = movie_response.json()
        num_movie_requests = len(new_movie_requests) - hold_requests['num_mh_requests']
        if num_movie_requests > 0:
            hold_requests['num_mh_requests'] += num_movie_requests

        logger.info(f"Received {num_movie_requests} events from /movie")
    else:
        logger.error("Failed to get events from /movie")

    new_book_max = [d["availability"] for d in new_book_requests]
    new_movie_max = [d["availability"] for d in new_movie_requests]

    current_book_max = hold_requests['max_bh_availability']
    current_movie_max = hold_requests['max_mh_availability']

    hold_requests['max_bh_availability'] = max(new_book_max, default=0) + current_book_max
    hold_requests['max_mh_availability'] = max(new_movie_max, default=0) + current_movie_max

    current_datetime = datetime.datetime.now()
    timestamp_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')

    hold_requests['last_updated'] = timestamp_datetime

    with open(app_config['datastore']['filename'], 'w') as file:
        json.dump(hold_requests, file, indent=2)
        logger.info("Successfully written into file")
        

def get_stats():
    logger.info("Request for statistics has started")

    if not os.path.exists(app_config['datastore']['filename']):
        logger.error("Statistics do not exist!")
        return f"Statistics do not exist", 404

    # Read the data from the file
    with open(app_config['datastore']['filename'], 'r') as file:
        current_stats = json.load(file)

    logger.debug(current_stats)
    logger.info("Request has completed!")

    return current_stats, 200

def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats, 'interval', seconds=app_config['scheduler']['period_sec'])
    sched.start()

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    # Run our standalone Gevent server
    init_scheduler()
    app.run(port=8100, use_reloader=False)
