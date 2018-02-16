import datetime
from dateutil import parser
from datetime import date


class ETLHelpers():

    # When timestamp is in the format "2017-01-18T05:38:52.172Z"
    @staticmethod
    def validate_date(timestamp):  # timestamp is in ISO time format
        datetime_object = str(parser.parse(timestamp)) # 2017-01-18 05:38:52.172000+00:00
        result = ETLHelpers.split_date(datetime_object, timestamp)
        return result

    # When timestamp is in the format "1516789266773649"
    @staticmethod
    def validate_date_epoch(timestamp):
        datetime_object = datetime.datetime.fromtimestamp(int(timestamp) / 1000000).strftime('%Y-%m-%d %H:%M:%S')
        # 2018-01-24 15:51:06
        result = ETLHelpers.split_date(datetime_object, timestamp)
        return result

    @staticmethod
    def split_date(time_obj, timestamp): # 2017-01-18 05:38:52.172000+00:00 or 2018-01-24 15:51:06
        validate_date = time_obj.split()[0]   #2017-01-18

        timestamp_split = validate_date.split('-')
        format_date = "%s%s%s" % (timestamp_split[0], timestamp_split[1], timestamp_split[2]) # 20170118

        date_dic = {"original_val": time_obj, "formatted_val":format_date}
        if validate_date > '1970-01-01':
            return date_dic
        else:
            return {"original_val": None, "formatted_val":None}

    # @staticmethod   # birthday is in ISO time format
    # def calculate_age(b_day):
    #     if b_day:
    #         datetime_object = str(parser.parse(b_day)).split()[0]
    #         timestamp_split = datetime_object.split('-')
    #         birthday = date(int(timestamp_split[0]), int(timestamp_split[1]), int(timestamp_split[2]))
    #         today = date.today()
    #         return today.year - birthday.year - ((today.month, today.day) < (birthday.month, birthday.day))
    #     else:
    #         return None
    #
    # @staticmethod
    # def is_birthday(b_day):
    #     if b_day:
    #         datetime_object = str(parser.parse(b_day)).split()[0]
    #         timestamp_split = datetime_object.split('-')
    #         birthday = date(int(timestamp_split[0]), int(timestamp_split[1]), int(timestamp_split[2]))
    #         today = date.today()
    #         if today.year == birthday.year and today.month == birthday.month and today.day == birthday.month:
    #             return True
    #         else:
    #             return False
    #     else:
    #         return None






