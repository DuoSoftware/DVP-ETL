__author__ = 'Surani Matharaarachchi'
__version__ = '1.0.1.0'

import urllib
from Helpers import ETLHelpers
from pygrametl.tables import Dimension, FactTable


class CDRClass():
    def __init__(self, json):
        self.cdr_json = json
        self.row = {}
        self.row2 = {}
        self.originatedLegs = []
        self.uuid = None
        self.call_uuid = None
        self.is_transfer_leg = False
        self.direction = None
        self.op_cat = None
        self.originatedLegsList = None
        self.member_uuid = None

    def extract_and_transform_cdr(self):

        var_sec = self.cdr_json['variables']
        call_flow_sec = self.cdr_json['callflow']

        if call_flow_sec and len(call_flow_sec) > 0:
            times_sec = call_flow_sec[0].get('times',None)
            caller_profile_sec = call_flow_sec[0].get('caller_profile', None)
            action_cat = var_sec.get('DVP_ACTION_CAT', None)
            self.op_cat = var_sec.get('DVP_OPERATION_CAT', None)
            adv_op_action = var_sec.get('DVP_ADVANCED_OP_ACTION', None)
            ards_added_time_stamp = var_sec.get('ards_added', None)
            queue_left_time_stamp = var_sec.get('ards_queue_left', None)
            ards_routed_time_stamp = var_sec.get('ards_routed', None)
            ards_resource_name = var_sec.get('ards_resource_name', None)
            ards_sip_name = var_sec.get('ARDS-SIP-Name', None)
            self.uuid = var_sec.get('uuid', None)
            self.call_uuid = var_sec.get('call_uuid', None)
            self.is_transfer_leg = var_sec.get('IsTransferLeg', False)
            bridge_uuid = var_sec.get('bridge_uuid', None)
            sip_to_user = caller_profile_sec.get('destination_number', None)
            sip_from_user = caller_profile_sec.get('caller_id_number', None)
            self.direction = var_sec.get('direction', None)
            dvp_call_direction = var_sec.get('DVP_CALL_DIRECTION', None)
            campaign_id = var_sec.get('CampaignId', None)
            campaign_name = var_sec.get('campaignname', None)
            is_agent_answered = False
            sip_resource = None
            is_queued = False
            app_id = var_sec.get('dvp_app_id', None)
            company_id = var_sec.get('companyid', None)
            tenant_id = var_sec.get('tenantid', None)
            b_unit = var_sec.get('business_unit', None)
            duration = var_sec.get('duration', None)
            bill_sec = var_sec.get('billsec', None)
            hold_sec = var_sec.get('hold_accum_seconds', None)
            progress_sec = var_sec.get('progresssec', None)
            answer_sec = var_sec.get('answersec', None)
            flow_bill_sec = var_sec.get('flow_billsec', None)
            progress_media_sec = var_sec.get('progress_mediasec', None)
            wait_sec = var_sec.get('waitsec', None)
            is_answered = False
            current_app = var_sec.get('current_application', None)
            conf_name = var_sec.get('DVP_CONFERENCE_NAME', None)
            self.member_uuid = var_sec.get('memberuuid', None)
            conference_uuid = var_sec.get('conference_uuid', None)
            start_epoch = var_sec.get('start_epoch', None)
            hangup_cause = var_sec.get('hangup_cause', None)
            switch_name = self.cdr_json.get('switchname', None)
            caller_context = caller_profile_sec.get('context', None)
            # member_uuid = caller_profile_sec.get('memberuuid', None)
            sip_hangup_disposition = var_sec.get('sip_hangup_disposition', None)
            OriginatedLegs = var_sec.get('originated_legs', None)
            agent_skill = None
            queue_time = None
            extra_data = None

            if ards_resource_name and dvp_call_direction == "inbound":
                sip_resource = ards_resource_name

            elif ards_sip_name and dvp_call_direction == 'inbound':
                sip_resource = ards_sip_name

            if action_cat == 'DIALER':
                if self.op_cat == 'AGENT':
                    sip_from_user = var_sec['sip_to_user']
                    sip_resource = var_sec['sip_to_user']
                    sip_to_user = var_sec['sip_from_user']
                elif (adv_op_action == 'BLAST' or adv_op_action == 'DIRECT' or adv_op_action =='IVRCALLBACK') and \
                                self.op_cat == 'CUSTOMER':

                    # NEED TO IMPLEMENT by FaceTone
                    sip_from_user = var_sec['origination_caller_id_number']
                    sip_to_user = var_sec['sip_to_user']

            elif self.direction == 'inbound' and dvp_call_direction == 'inbound':
                sip_from_user = var_sec['sip_from_user']  # get sip_from_user as from user for all inbound direction
                # calls

            if not sip_to_user or (action_cat == 'FORWARDING' and self.direction == 'inbound'):
                sip_to_user = urllib.unquote(var_sec['sip_to_user']).decode('utf8')

            if not sip_from_user:
                sip_from_user = urllib.unquote(var_sec['origination_caller_id_number']).decode('utf8')

            if not sip_to_user:
                sip_to_user = urllib.unquote(var_sec['dialed_user']).decode('utf8')

            if self.member_uuid:
                self.call_uuid = self.member_uuid

            if conference_uuid:
                self.call_uuid = conference_uuid

                sip_from_user = urllib.unquote(sip_from_user).decode('utf8')

            answered_time_stamp = times_sec.get('answered_time', None)
            if answered_time_stamp is not None and answered_time_stamp is not "0":
                answered_time = ETLHelpers.validate_date_epoch(answered_time_stamp)
                answer_date = answered_time["original_val"]
                answer_date_dim_id = answered_time["formatted_val"]
                answer_time_dim_id = answered_time["formatted_time"]

                if answered_time["original_val"]:
                    is_answered = True
            else:
                answer_date = None
                answer_date_dim_id = None
                answer_time_dim_id = None

            created_time_stamp = times_sec.get('created_time', None)
            if created_time_stamp is not None and created_time_stamp is not "0":
                created_time = ETLHelpers.validate_date_epoch(created_time_stamp)
                created_date = created_time["original_val"]
                created_date_dim_id = created_time["formatted_val"]
                created_time_dim_id = created_time["formatted_time"]

            elif start_epoch is not None and start_epoch is not "0":
                created_time = ETLHelpers.validate_date_epoch(start_epoch)
                created_date = created_time["original_val"]
                created_date_dim_id = created_time["formatted_val"]
                created_time_dim_id = created_time["formatted_time"]

            else:
                created_date = None
                created_date_dim_id = None
                created_time_dim_id = None

            bridged_time_stamp = times_sec.get('bridged_time', None)
            if bridged_time_stamp is not None and bridged_time_stamp is not "0":
                bridged_time = ETLHelpers.validate_date_epoch(bridged_time_stamp)
                bridge_date = bridged_time["original_val"]
                bridge_date_dim_id = bridged_time["formatted_val"]
                bridge_time_dim_id = bridged_time["formatted_val"]
            else:
                bridge_date = None
                bridge_date_dim_id = None
                bridge_time_dim_id = None

            hangup_time_stamp = times_sec.get('hangup_time', None)
            if hangup_time_stamp is not None and hangup_time_stamp is not "0":
                hangup_time = ETLHelpers.validate_date_epoch(hangup_time_stamp)
                hangup_date = hangup_time["original_val"]
                hangup_date_dim_id = hangup_time["formatted_val"]
                hangup_time_dim_id = hangup_time["formatted_val"]
            else:
                hangup_date = None
                hangup_date_dim_id = None
                hangup_time_dim_id = None

            if ards_added_time_stamp:
                is_queued = True

            if ards_added_time_stamp and queue_left_time_stamp:
                ards_added_time_sec = int(ards_added_time_stamp)
                queue_left_time_sec = int(queue_left_time_stamp)

                queue_time = queue_left_time_sec - ards_added_time_sec

            if ards_routed_time_stamp:
                is_agent_answered = True

            if not app_id:
                app_id = '-1'
            if not company_id:
                company_id = '-1'
            if not tenant_id:
                tenant_id = '-1'
            if not b_unit:
                b_unit = 'default'

            ards_skill_display = var_sec.get('ards_skill_display', None)

            if ards_skill_display:
                agent_skill = urllib.unquote(ards_skill_display).decode('utf8')

            if action_cat == 'CONFERENCE':
                extra_data = conf_name
            if action_cat:
                obj_category = action_cat
            if current_app == 'voicemail':
                obj_category = 'VOICEMAIL'
            elif adv_op_action == 'pickup':
                obj_category = 'PICKUP'
            if adv_op_action == 'INTERCEPT':
                obj_category = 'INTERCEPT'
            else:
                obj_category = 'DEFAULT'
            if action_cat == 'DIALER' and adv_op_action:
                obj_type = adv_op_action

            # Decode orginated legs
            if OriginatedLegs:
                decodedLegsStr = urllib.unquote(OriginatedLegs).decode('utf8')
                formattedStr = decodedLegsStr.replace("ARRAY::", "")
                self.originatedLegs = formattedStr.split("|:")
                legProperties = []
                leg_array = []

                for i in range(len(self.originatedLegs)):
                    leg = self.originatedLegs[i].split(";")[0]
                    if self.uuid != leg and leg not in leg_array:
                        leg_array.append(leg)
                        legProperties.append(self.originatedLegs[i].split(";")[0])
                        legProperties.append(",")
                self.originatedLegsList = ''.join(map(str, legProperties))

            else:
                self.originatedLegsList = None

            self.row["uuid"] = self.uuid
            self.row["CallUuid"] = self.call_uuid
            self.row["MemberUuid"] = self.member_uuid
            self.row["BridgeUuid"] = bridge_uuid
            self.row["SipFromUser"] = sip_from_user
            self.row["SipToUser"] = sip_to_user
            self.row["HangupCause"] = hangup_cause
            self.row["Direction"] = self.direction
            self.row["SwitchName"] = switch_name
            self.row["CallerContext"] = caller_context
            self.row["IsTransferLeg"] = self.is_transfer_leg
            self.row["IsAnswered"] = is_answered
            self.row["CreatedTime"] = created_date
            self.row["AnsweredTime"] = answer_date
            self.row["BridgedTime"] = bridge_date
            self.row["HangupTime"] = hangup_date
            self.row["CreatedDateDimId"] = created_date_dim_id
            self.row["AnsweredDateDimId"] = answer_date_dim_id
            self.row["BridgedDateDimId"] = bridge_date_dim_id
            self.row["HangupDateDimId"] = hangup_date_dim_id
            self.row["CreatedTimeDimId"] = created_time_dim_id
            self.row["AnsweredTimeDimId"] = answer_time_dim_id
            self.row["BridgedTimeDimId"] = bridge_time_dim_id
            self.row["HangupTimeDimId"] = hangup_time_dim_id
            self.row["Duration"] = duration
            self.row["BillSec"] = bill_sec
            self.row["HoldSec"] = hold_sec
            self.row["ProgressSec"] = progress_sec
            self.row["QueueSec"] = queue_time
            self.row["AnswerSec"] = answer_sec
            self.row["WaitSec"] = wait_sec
            self.row["ProgressMediaSec"] = progress_media_sec
            self.row["FlowBillSec"] = flow_bill_sec
            self.row["ObjClass"] = 'CDR'
            self.row["ObjType"] = self.op_cat
            self.row["ObjCategory"] = 'DEFAULT'
            self.row["CompanyId"] = company_id
            self.row["TenantId"] = tenant_id
            self.row["AppId"] = app_id
            self.row["AgentSkill"] = agent_skill
            self.row["OriginatedLegs"] = self.originatedLegsList
            self.row["DVPCallDirection"] = dvp_call_direction
            self.row["HangupDisposition"] = sip_hangup_disposition
            self.row["AgentAnswered"] = is_agent_answered
            self.row["IsQueued"] = is_queued
            self.row["SipResource"] = sip_resource
            self.row["CampaignId"] = campaign_id
            self.row["CampaignName"] = campaign_name
            self.row["BusinessUnit"] = b_unit
            self.row["ExtraData"] = extra_data

    def load_originated_legs(self):

        self.row2["uuid"] = self.uuid
        self.row2["CallUuid"] = self.call_uuid
        self.row2["MemberUuid"] = self.member_uuid
        self.row2["IsTransferLeg"] = self.is_transfer_leg
        self.row2["Direction"] = self.direction
        self.row2["ObjType"] = self.op_cat
        self.row2["originated_leg"] = self.originatedLegsList

        originated_legs_dimension = Dimension(
            name='"DimOriginatedLegs"',
            key='uuid',
            attributes=['CallUuid', 'Direction', 'MemberUuid', 'originated_leg', 'IsTransferLeg', 'ObjType'])

        originated_legs_dimension_temp = Dimension(
            name='"DimOriginatedLegsTemp"',
            key='uuid',
            attributes=['CallUuid', 'Direction', 'MemberUuid', 'originated_leg', 'IsTransferLeg', 'ObjType'])

        self.row2['uuid'] = originated_legs_dimension.ensure(self.row2)
        self.row2['uuid'] = originated_legs_dimension_temp.ensure(self.row2)

    def load_cdr(self):

        call_dimension = Dimension(
            name='"DimCall"',
            key='uuid',
            attributes=['SipFromUser', 'SipToUser', 'HangupCause', 'Direction', 'SwitchName', 'CallerContext',
                        'IsAnswered',  'Duration',  'ObjClass', 'ObjType', 'ObjCategory',
                        'AppId', 'AgentSkill', 'OriginatedLegs', 'IsTransferLeg', 'DVPCallDirection', 'HangupDisposition',
                        'AgentAnswered', 'IsQueued', 'SipResource', 'BusinessUnit', 'CampaignId', 'CampaignName',
                        'ExtraData'])

        call_fact_table = FactTable(
            name='"FactCall"',
            keyrefs=['uuid', 'CallUuid', 'BridgeUuid', 'CreatedDateDimId', 'AnsweredDateDimId', 'BridgedDateDimId',
                     'HangupDateDimId', 'CreatedTimeDimId', 'AnsweredTimeDimId', 'BridgedTimeDimId',
                     'HangupTimeDimId', 'CompanyId', 'TenantId'],
            measures=['CreatedTime', 'AnsweredTime', 'BridgedTime', 'HangupTime', 'BillSec', 'HoldSec', 'ProgressSec',
                      'QueueSec', 'AnswerSec', 'WaitSec', 'ProgressMediaSec', 'FlowBillSec'])

        call_dimension_temp = Dimension(
            name='"DimCallTemp"',
            key='uuid',
            attributes=['SipFromUser', 'SipToUser', 'HangupCause', 'Direction', 'SwitchName', 'CallerContext',
                        'IsAnswered', 'Duration', 'ObjClass', 'ObjType', 'ObjCategory',
                        'AppId', 'AgentSkill', 'OriginatedLegs', 'IsTransferLeg', 'DVPCallDirection',
                        'HangupDisposition',
                        'AgentAnswered', 'IsQueued', 'SipResource', 'BusinessUnit', 'CampaignId', 'CampaignName',
                        'ExtraData'])

        call_fact_table_temp = FactTable(
            name='"FactCallTemp"',
            keyrefs=['uuid', 'CallUuid', 'BridgeUuid', 'CreatedDateDimId', 'AnsweredDateDimId', 'BridgedDateDimId',
                     'HangupDateDimId', 'CreatedTimeDimId', 'AnsweredTimeDimId', 'BridgedTimeDimId',
                     'HangupTimeDimId', 'CompanyId', 'TenantId'],
            measures=['CreatedTime', 'AnsweredTime', 'BridgedTime', 'HangupTime', 'BillSec', 'HoldSec', 'ProgressSec',
                      'QueueSec', 'AnswerSec', 'WaitSec', 'ProgressMediaSec', 'FlowBillSec'])

        self.row['uuid'] = call_dimension.ensure(self.row)
        call_fact_table.insert(self.row)
        self.row['uuid'] = call_dimension_temp.ensure(self.row)
        call_fact_table_temp.insert(self.row)

    def generate_call_tables(self):

        self.extract_and_transform_cdr()
        self.load_originated_legs()
        self.load_cdr()

