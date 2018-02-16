__author__ = 'Surani Matharaarachchi'
__version__ = '1.0.0.0'

from Helpers import ETLHelpers
from pygrametl.tables import Dimension, FactTable
import psycopg2
import pygrametl


class TicketsClass():
    def __init__(self, json):
        self.tkt_json = json
        self.row = {}

    def extract_tickets(self):
        tkt_json = self.tkt_json

        self.row['engagement_session_id'] = tkt_json["engagement_session"]
        self.row['ticket_id'] = tkt_json["_id"]
        self.row['tenant'] = tkt_json.get("tenant", None)
        self.row['company'] = tkt_json.get("company", None)
        self.row['status'] = tkt_json.get("status", None)
        self.row['time_estimation'] = tkt_json.get("time_estimation", None)
        self.row['re_opens'] = tkt_json["ticket_matrix"].get("reopens", None)
        self.row['resolution_time'] = tkt_json["ticket_matrix"].get("resolution_time", None)  # Took as int
        self.row['worked_time'] = tkt_json["ticket_matrix"].get("worked_time", None)  # Took as int
        self.row['waited_time'] = tkt_json["ticket_matrix"].get("waited_time", None)  # Took as int
        self.row['groups'] = tkt_json["ticket_matrix"].get("groups", None)
        self.row['assignees'] = tkt_json["ticket_matrix"].get("assignees", None)
        self.row['replies'] = tkt_json["ticket_matrix"].get("replies", None)
        self.row['sla_violated'] = tkt_json["ticket_matrix"].get("sla_violated", None)
        self.row['requester'] = tkt_json.get("requester", None)
        self.row['tid'] = tkt_json.get("tid", None)
        self.row['active'] = tkt_json.get("active", None)
        self.row['is_sub_ticket'] = tkt_json.get("is_sub_ticket", None)
        self.row['sub_tickets'] = tkt_json.get("sub_tickets", None)
        self.row['subject'] = tkt_json.get("subject", None)
        self.row['reference'] = tkt_json.get("reference", None)
        self.row['description'] = tkt_json.get("description", None)
        self.row['channel'] = tkt_json.get("channel", None)
        self.row['assignee'] = tkt_json.get("assignee", None)
        self.row['assignee_group'] = tkt_json.get("assignee_group", None)
        self.row['slot_attachment'] = tkt_json.get("slot_attachment", None)
        self.row['comments'] = tkt_json.get("comments", None)
        self.row['custom_fields'] = tkt_json.get("custom_fields", None)
        self.row['merged_tickets'] = tkt_json.get("merged_tickets", None)
        self.row['related_tickets'] = tkt_json.get("related_tickets", None)
        self.row['attachments'] = tkt_json.get("attachments", None)
        self.row['watchers'] = tkt_json.get("watchers", None)
        self.row['collaborators'] = tkt_json.get("collaborators", None)
        self.row['status'] = tkt_json.get("status", None)
        self.row['priority'] = tkt_json.get("priority", None)
        self.row['sla'] = tkt_json.get("sla", None)
        self.row['form_submission'] = tkt_json.get("form_submission", None)
        self.row['submitter'] = tkt_json.get("submitter", None)
        self.row["ticket_type"] = tkt_json.get("type", None)
        self.row["tags"] = tkt_json.get("tags", None)
        self.row["event_author"] = tkt_json.get("author", None)
        self.row["security_level"] = tkt_json.get("security_level", None)
        self.row["isolated_tags"] = tkt_json.get("isolated_tags", None)
        self.row["status_type"] = tkt_json.get("status_type", None)

    def transform_tickets(self):
        tkt_json = self.tkt_json

        created_at = tkt_json.get("created_at", None)
        if created_at is not None and created_at is not "0":
            created_at = ETLHelpers.validate_date(created_at)
            self.row["created_at"] = created_at["original_val"]
            self.row["created_at_dim_id"] = created_at["formatted_val"]
        else:
            self.row["created_at"] = None
            self.row["created_at_dim_id"] = None

        due_at = tkt_json.get("due_at", None)
        if due_at is not None and due_at is not "0":
            due_at = ETLHelpers.validate_date(due_at)
            self.row['due_at'] = due_at["original_val"]
            self.row['due_at_dim_id'] = due_at["formatted_val"]
        else:
            self.row["due_at"] = None
            self.row["due_at_dim_id"] = None

        updated_at = tkt_json.get("last_updated", None)
        if updated_at is not None and updated_at is not "0":
            updated_at = ETLHelpers.validate_date(updated_at)
            self.row['updated_at'] = updated_at["original_val"]
            self.row['updated_at_dim_id'] = updated_at["formatted_val"]
        else:
            self.row["updated_at"] = None
            self.row["updated_at_dim_id"] = None

        last_status_changed = tkt_json["ticket_matrix"].get("last_status_changed", None)
        if last_status_changed is not None and last_status_changed is not "0":
            last_status_changed = ETLHelpers.validate_date(last_status_changed)
            self.row['last_status_changed'] = last_status_changed["original_val"]
            self.row['last_status_changed_dim_id'] = last_status_changed["formatted_val"]
        else:
            self.row["last_status_changed"] = None
            self.row["last_status_changed_dim_id"] = None

    def load_tickets(self):
        ticket_dimension = Dimension(
            name='"DimTicket"',
            key='ticket_id',
            attributes=['active', 'tid', 'is_sub_ticket', 'subject', 'reference', 'description', 'priority', 'requester',
                        'submitter', 'collaborators', 'watchers', 'attachments', 'sub_tickets', 'related_tickets',
                        'merged_tickets', 'isolated_tags', 'sla', 'comments', 'tags', 'ticket_type', 'status_type',
                        'channel', 'company', 'tenant', 'assignee', 'assignee_group', 'security_level', 'event_author'])

        ticket_fact_table = FactTable(
            name='"FactTicket"',
            keyrefs=['engagement_session_id', 'ticket_id', 'created_at_dim_id', 'updated_at_dim_id', 'due_at_dim_id'],
            measures=['created_at', 'updated_at', 'due_at', 'time_estimation', 're_opens', 'waited_time', 'worked_time',
                      'resolution_time'])

        # Updates the row with the primary keys of each dimension while at the same time inserting new data into
        # each dimension

        self.row['ticket_dim_id'] = ticket_dimension.ensure(self.row)
        ticket_fact_table.insert(self.row)

    def generate_ticket_tables(self):
            self.extract_tickets()
            self.transform_tickets()
            return self.load_tickets()
