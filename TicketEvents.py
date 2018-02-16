__author__ = 'Surani Matharaarachchi'
__version__ = '1.0.0.0'

from Helpers import ETLHelpers
from pygrametl.tables import Dimension, FactTable


class TicketEventsClass():
    def __init__(self, arr, tkt_id):
        self.tkt_event_arr = arr
        self.tkt_id = tkt_id
        self.row = {}

    def extract_ticket_events(self, element):
        event_json = element
        self.row["ticket_id"] = self.tkt_id
        self.row["ticket_event_id"] = event_json["_id"]
        self.row["event_type"] = event_json.get("type", None)
        self.row["event_author"] = event_json.get("author", None)
        self.row["event_body"] = str(event_json.get("body", None))
        # print self.row

    def transform_ticket_events(self, element):
        event_json = element
        event_created_at = event_json.get("create_at", None)
        if event_created_at is not None and event_created_at is not "0":
            updated_at = ETLHelpers.validate_date(event_created_at)
            self.row['event_created_at'] = updated_at["original_val"]
            self.row['event_created_at_dim_id'] = updated_at["formatted_val"]
        else:
            self.row["event_created_at"] = None
            self.row["event_created_at_dim_id"] = None

    def load_ticket_events(self):
        ticket_event_dimension = Dimension(
            name='"DimTicketEvent"',
            key='ticket_event_id',
            attributes=['event_type', 'event_author', 'event_body'])

        ticket_event_fact_table = FactTable(
            name='"FactTicketEvent"',
            keyrefs=['ticket_id', 'ticket_event_id'],
            measures=['event_created_at_dim_id', 'event_created_at'])

        # Update the row with the primary keys of each dimension while at the same time inserting new data into
        # each dimension

        ticket_event_dimension.ensure(self.row)
        ticket_event_fact_table.insert(self.row)

    def generate_ticket_event_tables(self, arr):
        for element in arr:
            self.extract_ticket_events(element)
            self.transform_ticket_events(element)
            self.load_ticket_events()

