__author__ = 'Surani Matharaarachchi'
__version__ = '1.0.1.0'

import psycopg2
from RabbitMQConnection import RabbitMQConnection
import CDR_Extended as cdr
import Engagements as eng
import Tickets as tkt
import ExternalUsers as exuser
import pygrametl
import json
import string
import ConfigParser
import logging
import sys
import threading
from multiprocessing import Process

log_path = 'logs/ETL.log'
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.info('--------------------------------------  ETL  -----------------------------------------------')
logger.info('Starting ETL')


class ETLClass():
    @staticmethod
    def get_conf(filename, section):

        Config = ConfigParser.ConfigParser()
        Config.optionxform = str  # This makes configparser not to lowercase the keys
        Config.read(filename)

        dict1 = {}
        options = Config.options(section)
        for option in options:
            try:
                dict1[option] = Config.get(section, option)
                if dict1[option] == -1:
                    print("skip: %s" % option)
            except Exception:
                print("exception on %s!" % option)
                dict1[option] = None
        return dict1

    def __init__(self, queue_name, body, delivery_tag):

        try:
            self.json = json.loads(string.replace(body, '\\', ''))
        except ValueError as e:
            self.json = json.loads(body)

        self.data_type = queue_name
        self.delivery_tag = delivery_tag

    def load_data(self):

        if self.data_type == "DigInCDRs":
            result = cdr.CDRClass(self.json).generate_call_tables()

        elif self.data_type == "DigInEngagements":
            result = eng.EngagementClass(self.json).generate_engagement_tables()

        elif self.data_type == "DigInTickets":
            result = tkt.TicketsClass(self.json).generate_ticket_tables()

        elif self.data_type == "DigInExternalUsers":
            result = exuser.ExternalUserClass(self.json).generate_external_user_tables()
        else:
            return

if __name__ == "__main__":

    rmqconf = ETLClass.get_conf("Config.ini", "RabbitMQ")
    rmq_host = rmqconf['rmq_host']
    rmq_user = rmqconf['rmq_user']
    rmq_password = rmqconf['rmq_password']
    rmq_port = int(rmqconf['rmq_port'])
    rmq_vhost = rmqconf['rmq_vhost']

    rmq = RabbitMQConnection(rmq_host, rmq_user, rmq_password, rmq_port, rmq_vhost)

    pg_constr = ETLClass.get_conf("Config.ini", "Warehouse")
    pgconn = psycopg2.connect(pg_constr['constr'])
    conn = pygrametl.ConnectionWrapper(connection=pgconn)

    def callback(queue_name, body, delivery_tag):
        logger.info("Massage received")

        try:
            result = [threading.Thread(target=ETLClass(queue_name, body, delivery_tag).load_data(), args=())]
            for t in result:
                t.start()

            for t in result:
                t.join()

            conn.commit()

        except psycopg2.IntegrityError as e:
            conn.rollback()
            print e
            rmq.acknowledge_task(delivery_tag)
            logger.error(sys.exc_info())
        except KeyError:
            conn.rollback()
            rmq.acknowledge_task(delivery_tag)
            logger.error(sys.exc_info())
        except Exception as e:
            print e
            conn.rollback()
            rmq.acknowledge_task(delivery_tag)
            logger.error(sys.exc_info())
        else:
            rmq.acknowledge_task(delivery_tag)
            logger.info(result)


    def get_data_from_queues():
        rmq.register_queues(['DigInCDRs', 'DigInEngagements', 'DigInTickets', 'DigInExternalUsers'])
        rmq.register_callback(callback)
        rmq.start_client()

    try:
        result = [threading.Thread(target=get_data_from_queues(), args=())]
        for t in result:
            t.start()

        for t in result:
            t.join()

    except Exception as e:
        print e
        logger.error(sys.exc_info())
