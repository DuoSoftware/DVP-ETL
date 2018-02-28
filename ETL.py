__author__ = 'Surani Matharaarachchi'
__version__ = '1.0.0.0'

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
        self.json = json.loads(string.replace(body, '\\', ''))
        self.data_type = queue_name
        self.delivery_tag = delivery_tag

    def load_data(self):

        pg_constr = ETLClass.get_conf("Config.ini", "Warehouse")
        pgconn = psycopg2.connect(pg_constr['constr'])
        conn = pygrametl.ConnectionWrapper(connection=pgconn)
        try:
            if self.data_type == "cdr":
                result = cdr.CDRClass(self.json).generate_call_tables()

            elif self.data_type == "engagement":
                result = eng.EngagementClass(self.json).generate_engagement_tables()

            elif self.data_type == "tickets":
                tkt_json = self.json
                result = tkt.TicketsClass(tkt_json).generate_ticket_tables()

            elif self.data_type == "external_users":
                result = exuser.ExternalUserClass(self.json).generate_external_user_tables()
            else:
                return
            conn.commit()
            return result
        except psycopg2.IntegrityError:
            conn.rollback()
            raise
        except KeyError:
            conn.rollback()
            raise
        except:   
            conn.rollback()
            raise


if __name__ == "__main__":

    rmqconf = ETLClass.get_conf("Config.ini", "RabbitMQ")
    rmq_host = rmqconf['rmq_host']
    rmq_user = rmqconf['rmq_user']
    rmq_password = rmqconf['rmq_password']
    rmq_port = int(rmqconf['rmq_port'])
    rmq_vhost = rmqconf['rmq_vhost']

    rmq = RabbitMQConnection(rmq_host, rmq_user, rmq_password, rmq_port, rmq_vhost)

    def callback(queue_name, body, delivery_tag):  # Data to be defined

        try:
            result = ETLClass(queue_name, body, delivery_tag).load_data()
        except Exception as e:
            result = None
            logger.info(e)

        if result is psycopg2.IntegrityError:
            rmq.acknowledge_task(delivery_tag)
        else:
            rmq.acknowledge_task(delivery_tag)

    rmq.register_queues(['cdr', 'engagement', 'tickets', 'external_users'])
    rmq.register_callback(callback)
    rmq.start_client()
