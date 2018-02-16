__author__ = 'Nimeshka Srimal'
__version__ = '1.0.0.0'

"""
Wrapper class for Pika library to connect to RabbitMQ.
"""

import pika
import logging
import time


class RabbitMQConnection(object):

    def __init__(self, host, user, password, port, vhost):
        credentials = pika.PlainCredentials(user, password)
        # parameters = pika.ConnectionParameters(host, port, vhost, credentials, heartbeat_interval=20, connection_attempts=3)
        parameters = pika.ConnectionParameters(host, port, vhost, credentials, heartbeat_interval=500)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=1)

        self.queues = []
        self.callback_method = None

    @staticmethod
    def debug(on = False):
        if on:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.NOTSET)

    # invoke the callback method passed by the client with our arguments
    def __callback(self, ch, method, properties, body):
        print 'received %s .. from %s' % (body, method.routing_key)
        self.callback_method(method.routing_key, body, method.delivery_tag)

    def register_queues(self, queues=[]):
        self.queues = queues

    def register_callback(self, callback):
        self.callback_method = callback

    def acknowledge_task(self, delivery_tag):
        self.channel.basic_ack(delivery_tag=delivery_tag)

    def start_client(self):

        if len(self.queues) > 0:
            for q in self.queues:
                self.channel.queue_declare(queue=q)
                self.channel.basic_consume(self.__callback, queue=q)
        else:
            raise ValueError("Queue list must contain atleast one queue.")

        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print "Closing connection.."
            self.connection.close()


