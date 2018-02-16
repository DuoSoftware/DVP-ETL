
__version__ = '1.0.0.0'

from Helpers import ETLHelpers
from pygrametl.tables import Dimension, FactTable


class ExternalUserClass():

    def __init__(self, json):
        self.exusers_json = json
        self.row = {}

    def extract_external_users(self):
        exusers_json = self.exusers_json

        self.row["external_user_id"] = exusers_json["_id"]
        self.row["title"] = exusers_json.get("title", None)
        self.row["name"] = exusers_json.get("name", None)
        self.row["avatar"] = exusers_json.get("avatar", None) # Is this need to be added?
        self.row["birthday"] = exusers_json.get("birthday", None)
        self.row["gender"] = exusers_json.get("gender", None)
        self.row["first_name"] = exusers_json.get("firstname", None)
        self.row["last_name"] = exusers_json.get("lastname", None)
        self.row["locale"] = exusers_json.get("locale", None)
        self.row["ssn"] = exusers_json.get("ssn", None)
        self.row["phone"] = exusers_json.get("phone", None)
        self.row["email"] = exusers_json.get("email", None)
        self.row["company"] = exusers_json.get("company", None)
        self.row["tenant"] = exusers_json.get("tenant", None)
        self.row["custom_fields"] = exusers_json.get("custom_fields", None)
        self.row["tags"] = exusers_json.get("tags", None)
        self.row["contacts"] = exusers_json.get("contacts", None)
        self.row["email"] = exusers_json.get("email", None)
        self.row["landnumber"] = exusers_json.get("landnumber", None)
        self.row["facebook"] = exusers_json.get("facebook", None)
        self.row["twitter"] = exusers_json.get("twitter", None)
        self.row["linkedin"] = exusers_json.get("linkedin", None)
        self.row["googleplus"] = exusers_json.get("googleplus", None)
        self.row["skype"] = exusers_json.get("skype ", None)
        self.row["password"] = exusers_json.get("password", None)
        self.row["thirdpartyreference"] = exusers_json.get("thirdpartyreference", None)
        self.row["zipcode"] = exusers_json["address"].get("zipcode", None)
        self.row["address_number"] = exusers_json["address"].get("number", None)
        self.row["city"] = exusers_json["address"].get("city", None)
        self.row["province"] = exusers_json["address"].get("province", None)
        self.row["country"] = exusers_json["address"].get("country", None)

    def transform_external_users(self):
        exusers_json = self.exusers_json

        created_at = exusers_json.get("created_at", None)
        if created_at is not None and created_at is not "0":
            created_at = ETLHelpers.validate_date(created_at)
            self.row["created_at"] = created_at["original_val"]
            self.row["created_at_dim_id"] = created_at["formatted_val"]
        else:
            self.row["created_at"] = None
            self.row["created_at_dim_id"] = None

        updated_at = exusers_json.get("updated_at", None)
        if updated_at is not None and updated_at is not "0":
            updated_at = ETLHelpers.validate_date(updated_at)
            self.row["updated_at"] = updated_at["original_val"]
            self.row["updated_at_dim_id"] = updated_at["formatted_val"]
        else:
            self.row["updated_at"] = None
            self.row["updated_at_dim_id"] = None

    def load_external_users(self):

        external_users_dimension = Dimension(
            name='"DimExternalUsers"',
            key='external_user_id',
            attributes=['title', 'name', 'avatar', 'birthday', 'gender', 'first_name', 'last_name',
                        'locale', 'ssn', 'password',  'phone', 'email', 'landnumber', 'facebook', 'twitter', 'linkedin',
                        'googleplus', 'skype', 'thirdpartyreference', 'company', 'tenant', 'custom_fields', 'tags',
                        'contacts', 'zipcode', 'address_number', 'city', 'province', 'country'])

        external_users_fact_table = FactTable(
            name='"FactExternalUsers"',
            keyrefs=['external_user_id', 'created_at_dim_id', 'updated_at_dim_id'],
            measures=['created_at', 'updated_at'])

        # Update the row with the primary keys of each dimension while at the same time inserting new data into
        # each dimension

        self.row['external_users_dim_id'] = external_users_dimension.ensure(self.row)
        external_users_fact_table.insert(self.row)

    def generate_external_user_tables(self):

            self.extract_external_users()
            self.transform_external_users()
            self.load_external_users()

