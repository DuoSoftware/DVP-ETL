__author__ = 'Surani Matharaarachchi'
__version__ = '1.0.1.0'

import psycopg2
from RabbitMQConnection import RabbitMQConnection
import CDR as cdr
import Engagements as eng
import Tickets as tkt
import ExternalUsers as exuser
import pygrametl
import json
import string
import ConfigParser
import logging
import sys
import os

log_path = 'logs/ETL.log'
try:
    os.makedirs('logs')
except OSError:
    if not os.path.isdir('logs'):
        raise

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
        # todo: Handle other types of Exceptions
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
    rmq_host = os.environ[rmqconf['rmq_host']]
    rmq_user = os.environ[rmqconf['rmq_user']]
    rmq_password = os.environ[rmqconf['rmq_password']]
    rmq_port = int(os.environ[rmqconf['rmq_port']])
    rmq_vhost = os.environ[rmqconf['rmq_vhost']]

    rmq = RabbitMQConnection(rmq_host, rmq_user, rmq_password, rmq_port, rmq_vhost)

    pg_constr = ETLClass.get_conf("Config.ini", "Warehouse")
    pgconn = psycopg2.connect(os.environ[pg_constr['constr']])
    conn = pygrametl.ConnectionWrapper(connection=pgconn)

    def callback(queue_name, body, delivery_tag):
        logger.info("Massage received")
        try:
            result = ETLClass(queue_name, body, delivery_tag).load_data()
            conn.commit()
        # todo: Handle other types of Exceptions
        except psycopg2.IntegrityError as e:
            conn.rollback()
            print e
            logger.error(sys.exc_info())
        except KeyError as e:
            print e
            conn.rollback()
            logger.error(sys.exc_info())
        except Exception as e:
            print e
            conn.rollback()
            logger.error(sys.exc_info())
        else:
            logger.info(result)
        rmq.acknowledge_task(delivery_tag)

    # rmq.register_queues(['tickets'])
    rmq.register_queues(['DigInCDRs', 'DigInEngagements', 'DigInTickets', 'DigInExternalUsers'])
    rmq.register_callback(callback)
    rmq.start_client()

