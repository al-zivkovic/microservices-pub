import datetime
import json

import connexion
from connexion import NoContent
import swagger_ui_bundle

import mysql.connector 
import pymysql
import yaml
import logging
import logging.config

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from base import Base
from buy import Buy
from sell import Sell

import pykafka
from pykafka import KafkaClient
from pykafka.common import OffsetType

import threading
from threading import Thread

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

DB_ENGINE = create_engine(f"mysql+pymysql://{app_config['user']}:{app_config['password']}@{app_config['hostname']}:{app_config['port']}/{app_config['db']}")
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

def process_messages():
    # TODO: create KafkaClient object assigning hostname and port from app_config to named parameter "hosts"
    # and store it in a variable named 'client'
    client = KafkaClient(hosts=f"{app_config['events']['hostname']}:{app_config['events']['port']}")
    # TODO: index into the client.topics array using topic from app_config
    # and store it in a variable named topic
    topic = client.topics[app_config['events']['topic']]

    # Notes:
    #
    # An 'offset' in Kafka is a number indicating the last record a consumer has read,
    # so that it does not re-read events in the topic
    #
    # When creating a consumer object,
    # reset_offset_on_start = False ensures that for any *existing* topics we will read the latest events
    # auto_offset_reset = OffsetType.LATEST ensures that for any *new* topic we will also only read the latest events
    
    messages = topic.get_simple_consumer( 
        reset_offset_on_start = False, 
        auto_offset_reset = OffsetType.LATEST
    )

    for msg in messages:
        # This blocks, waiting for any new events to arrive
        # TODO: decode (utf-8) the value property of the message, store in a variable named msg_str
        msg_str = msg.value.decode('utf-8')
        # TODO: convert the json string (msg_str) to an object, store in a variable named msg
        msg = json.loads(msg_str)
        # TODO: extract the payload property from the msg object, store in a variable named payload
        payload = msg['payload']
        # TODO: extract the type property from the msg object, store in a variable named msg_type
        msg_type = msg['type']
        # TODO: create a database session
        session = DB_SESSION()
        # TODO: log "CONSUMER::storing buy event"
        # TODO: log the msg object
        logger.debug("CONSUMER::storing buy event")
        logger.debug(msg)
        # TODO: if msg_type equals 'buy', create a Buy object and pass the properties in payload to the constructor
        # if msg_type equals sell, create a Sell object and pass the properties in payload to the constructor
        if msg_type == 'buy':
            buy = Buy(buy_id=payload['buy_id'], item_name=payload['item_name'], item_price=payload['item_price'], buy_qty=payload['buy_qty'], trace_id=payload['trace_id'])
        elif msg_type == 'sell':
            sell = Sell(sell_id=payload['sell_id'], item_name=payload['item_name'], item_price=payload['item_price'], sell_qty=payload['sell_qty'], trace_id=payload['trace_id'])
        # TODO: session.add the object you created in the previous step
        if msg_type == 'buy':
            session.add(buy)
        elif msg_type == 'sell':
            session.add(sell)
        # TODO: commit the session
        session.commit()
    # TODO: call messages.commit_offsets() to store the new read position
    messages.commit_offsets()
# Endpoints
def buy(body):
    # TODO: copy over code from previous version of storage
# end
    my_session = DB_SESSION()
    buy = Buy(buy_id=body['buy_id'], item_name=body['item_name'], item_price=body['item_price'], buy_qty=body['buy_qty'], trace_id=body['trace_id'])
    my_session.add(buy)
    my_session.commit()
    my_session.close()
    logger.debug(f"Stored buy event with trace id {body['trace_id']}")
    return NoContent, 201

def get_buys(timestamp):
    # TODO: copy over code from previous version of storage
    session = DB_SESSION()  
    data = []
    rows = session.query(Buy).filter(Buy.date_created >= timestamp)

    for row in rows:
        data.append(row.to_dict())

    return data, 200

def sell(body):
    # TODO: copy over code from previous version of storage
# end
    my_session = DB_SESSION()
    sell = Sell(sell_id=body['sell_id'], item_name=body['item_name'], item_price=body['item_price'], sell_qty=body['sell_qty'], trace_id=body['trace_id'])
    my_session.add(sell)
    my_session.commit()
    my_session.close()
    logger.debug(f"Stored sell event with trace id {body['trace_id']}")
    return NoContent, 201

def get_sells(timestamp):
    # TODO: copy over code from previous version of storage
    session = DB_SESSION()
    data = []
    rows = session.query(Sell).filter(Sell.date_created >= timestamp)

    for row in rows:
        data.append(row.to_dict())

    return data, 200

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('openapi.yaml', base_path="/storage", strict_validation=True, validate_responses=True)

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basic')

if __name__ == "__main__":
    tl = Thread(target=process_messages)
    tl.daemon = True
    tl.start()
    app.run(port=8090)
