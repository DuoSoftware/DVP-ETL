__author__ = 'Surani Matharaarachchi'
__version__ = '1.0.0.0'

from Helpers import ETLHelpers
from pygrametl.tables import Dimension, FactTable


class EngagementClass():
    def __init__(self, json):
        self.eng_json = json
        self.row = {}

    def extract_engagements(self):
        eng_json = self.eng_json

        self.row['engagement_id'] = eng_json["engagement_id"]
        self.row['profile'] = eng_json.get("profile", None)
        self.row['channel_from'] = eng_json.get("channel_from", None)
        self.row['channel_to'] = eng_json.get("channel_to", None)
        self.row['company'] = eng_json.get("company", None)
        self.row['has_profile'] = eng_json.get("has_profile", None)
        self.row['tenant'] = eng_json.get("tenant", None)
        self.row['notes'] = eng_json.get("notes", None)
        self.row['channel'] = eng_json.get("channel", None)

    def transform_engagement(self):
        eng_json = self.eng_json

        created_at = eng_json.get("created_at", None)
        if created_at is not None and created_at is not "0":
            created_at = ETLHelpers.validate_date(eng_json["created_at"])
            self.row['created_at'] = created_at["original_val"]
            self.row['created_at_dim_id'] = created_at["formatted_val"]
            self.row['created_time_dim_id'] = created_at["formatted_time"]
        else:
            self.row["created_at"] = None
            self.row["created_at_dim_id"] = None
            self.row["created_time_dim_id"] = None

        updated_at = eng_json.get("updated_at", None)
        if updated_at is not None and updated_at is not "0":
            updated_at = ETLHelpers.validate_date(updated_at)
            self.row['updated_at'] = updated_at["original_val"]
            self.row['updated_at_dim_id'] = updated_at["formatted_val"]
            self.row['updated_time_dim_id'] = updated_at["formatted_time"]
        else:
            self.row["updated_at"] = None
            self.row["updated_at_dim_id"] = None
            self.row["updated_time_dim_id"] = None

    def load_engagement(self):
        engagement_dimension = Dimension(
            name='"DimEngagement"',
            key='engagement_id',
            attributes=['profile', 'channel_from', 'channel_to', 'company', 'has_profile', 'tenant',  'notes', 'channel'])

        engagement_fact_table = FactTable(
            name='"FactEngagement"',
            keyrefs=['engagement_id', 'created_at_dim_id', 'updated_at_dim_id', 'created_time_dim_id', 'updated_time_dim_id'],
            measures=['updated_at', 'created_at'])

        self.row['eng_dim_id'] = engagement_dimension.ensure(self.row)
        engagement_fact_table.insert(self.row)
        return True

    def generate_engagement_tables(self):
        self.extract_engagements()
        self.transform_engagement()
        self.load_engagement()
