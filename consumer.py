import os
import numpy as np
import kombu


from kombu.mixins import ConsumerMixin
from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path('.env')
load_dotenv(dotenv_path=dotenv_path)

RABBIT_MQ_URL = os.getenv('RABBIT_MQ_URL')
RABBIT_MQ_USERNAME = os.getenv('RABBIT_MQ_USERNAME')
RABBIT_MQ_PASSWORD = os.getenv('RABBIT_MQ_PASSWORD')
RABBIT_MQ_EXCHANGE_NAME = os.getenv("RABBIT_MQ_EXCHANGE_NAME")
RABBIT_MQ_QUEUE_NAME = os.getenv("RABBIT_MQ_QUEUE_NAME")


BASE_URL = "http://localhost:8000"

rabbit_url = f'amqp://{RABBIT_MQ_USERNAME}:{RABBIT_MQ_PASSWORD}@{RABBIT_MQ_URL}//'
# Kombu Message Consuming Worker


class Worker(ConsumerMixin):

    def __init__(self, connection, queues):
        self.connection = connection
        self.queues = queues

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=self.queues,
                         callbacks=[self.on_message],
                         accept=['application/json'])]

    def on_message(self, body, message):
        # get the original jpeg byte array size

        print(body)

        message.ack()


def run():
    exchange = kombu.Exchange(RABBIT_MQ_EXCHANGE_NAME, type="direct")
    queues = [kombu.Queue(RABBIT_MQ_QUEUE_NAME, exchange, routing_key="alarm")]

    with kombu.Connection(rabbit_url, heartbeat=4, ssl=True) as conn:
        worker = Worker(conn, queues)
        worker.run()


if __name__ == "__main__":
    run()
