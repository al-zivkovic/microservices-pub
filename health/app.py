import connexion
import json
import logging
import logging.config
import requests
import yaml

statuses = {
    "receiver": "",
    "storage": "",
    "processing": ""
}


def check():
    # TODO - use try except block(s) to send a GET request to each of your services /health endpoints (receiver, storage, and processing)
    # For each service, check if the response has status code 200, e.g. res.status_code == 200
    # If the status code for a given service is 200, index into the statuses dict and assign the string "Up" to the correct service key
    # If the status is not 200, or an exception is thrown because the request cannot be completed, index into the statuses dict and assign the string "Down" to the correct key
    try:
        receiver = requests.get("http://localhost:8080/health")
        if receiver.status_code == 200:
            statuses["receiver"] = "Up"
        else:
            statuses["receiver"] = "Down"
        storage = requests.get("http://localhost:8090/health")
        if storage.status_code == 200:
            statuses["storage"] = "Up"
        else:
            statuses["storage"] = "Down"
        processing = requests.get("http://localhost:8100/health")
        if processing.status_code == 200:
            statuses["processing"] = "Up"
        else:
            statuses["processing"] = "Down"
    except:
        statuses["receiver"] = "Down"
        statuses["storage"] = "Down"
        statuses["processing"] = "Down"

    return statuses  # ignore the instructions to convert this to JSON and leave as-is


app = connexion.FlaskApp(__name__, specification_dir='')

# if you are deploying this to your VM, make sure to add base_path="/health" to the add_api method (and update your NGINX config to proxy requests to the health service)
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basic')

if __name__ == "__main__":
    app.run(port=8110)
