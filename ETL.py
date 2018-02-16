__author__ = 'Surani Matharaarachchi'
__version__ = '1.0.0.0'

import psycopg2
from RabbitMQConnection import RabbitMQConnection
import CDR as cdr
import Engagements as eng
import Tickets as tkt
import TicketEvents as tktEvent
import ExternalUsers as exuser
import pygrametl
import json
import string


class ETLClass():
    def __init__(self, queue_name, body, delivery_tag):
        self.json = json.loads(string.replace(body, '\\', ''))
        self.data_type = queue_name
        self.delivery_tag = delivery_tag

    def load_data(self):

        pg_constr = "dbname=DigIn user=duo password=DigIn123 host=35.201.165.0 port=3245"
        pgconn = psycopg2.connect(pg_constr)
        conn = pygrametl.ConnectionWrapper(connection=pgconn)
        try:
            if self.data_type == "cdr":
                result = cdr.CDRClass(self.json).generate_call_tables()

            elif self.data_type == "engagement":
                result = eng.EngagementClass(self.json).generate_engagement_tables()

            elif self.data_type == "tickets":
                tkt_json = self.json
                result = tkt.TicketsClass(tkt_json).generate_ticket_tables()

                tkt_event_arr = self.json["events"]
                tkt_id = self.json["_id"]
                result = tktEvent.TicketEventsClass(tkt_event_arr, tkt_id).generate_ticket_event_tables(tkt_event_arr)

            elif self.data_type == "external_users":
                result = exuser.ExternalUserClass(self.json).generate_external_user_tables()
            else:
                return
            conn.commit()
            return result
        except psycopg2.IntegrityError as e:
            conn.rollback()
            return e


if __name__ == "__main__":

    rmq_host = '35.201.165.0'
    rmq_user = 'admin'
    rmq_password = 'DigIn123'
    rmq_port = 5672
    rmq_vhost = '/'

    rmq = RabbitMQConnection(rmq_host, rmq_user, rmq_password, rmq_port, rmq_vhost)

    def callback(queue_name, body, delivery_tag):  # Data to be defined
        result = ETLClass(queue_name, body, delivery_tag).load_data()

        if result is psycopg2.IntegrityError:
            rmq.acknowledge_task(delivery_tag)
        else:
            rmq.acknowledge_task(delivery_tag)

    rmq.register_queues(['cdr', 'engagement', 'tickets', 'external_users'])
    rmq.register_callback(callback)
    rmq.start_client()
