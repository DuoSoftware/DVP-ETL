CREATE OR REPLACE FUNCTION public.ft_cdr_report()
 RETURNS TABLE(dvpcalldirection character varying, sipfromuser character varying, siptouser character varying, receivedby character varying, agentskill character varying, agentanswered boolean, createdtime character varying, fulldatesl character varying, year integer, month integer, monthname character varying, dayofmonth integer, quarter integer, quartername character varying, isweekday boolean, duration integer, billsec integer, waitsec integer, queuesec integer, holdsec integer, objtype character varying, objcategory character varying, businessunit character varying)
 LANGUAGE plpgsql
AS $function$

DECLARE 
    var_r record;
 
BEGIN
 FOR var_r IN(
    SELECT p.dvpcalldirection AS dvpcalldirection,
    p.sipfromuser AS sipfromuser,
    p.siptouser AS siptouser,
    p.receivedby AS receivedby,
    p.agentskill AS agentskill,
    p.agentanswered AS agentanswered, 
    p.createdtime AS createdtime,
    c.fulldatesl AS fulldatesl,
    c.year AS year,
    c.month AS month,
    c.monthname AS monthname,
    c.dayofmonth AS dayofmonth,
    c.quarter AS quarter,
    c.quartername AS quartername,
    c.isweekday AS isweekday,
    p.duration AS duration,
    p.billsec AS billsec,
    p.waitsec AS waitsec,
    p.queuesec AS queuesec,
    p.holdsec AS holdsec,
    p.objtype AS objtype,
    p.objcategory AS objcategory,
    p.businessunit AS businessunit
   FROM "ProcessedCalls" p
     LEFT JOIN "DimDate" c ON p.createdtimedimid::text = c.datedimid::text
     INNER JOIN "DimCompany" e ON p.companyid = e.companyid AND p.tenantid = e.tenantid
UNION
 SELECT a.m_dvpcalldirection AS dvpcalldirection,
    a.m_sipfromuser AS sipfromuser,
    a.m_siptouser AS siptouser,
    a.m_receivedby AS receivedby,
    a.m_agentskill AS agentskill,
    a.m_agentanswered AS agentanswered,
    a.m_createdtime AS createdtime,
    c.fulldatesl AS fulldatesl,
    c.year AS year,
    c.month AS month,
    c.monthname AS monthname,
    c.dayofmonth AS dayofmonth,
    c.quarter AS quarter,
    c.quartername AS quartername,
    c.isweekday As isweekday,
    a.m_duration AS duration,
    a.m_billsec AS billsec,
    a.m_waitsec AS waitsec,
    a.m_queuesec AS queuesec,
    a.m_holdsec AS holdsec,
    a.m_objtype AS bjtype,
    a.m_objcategory AS objcategory,
    a.m_businessunit AS businessunit
   FROM get_call_details(0, false) a(m_uuid, m_recordinguuid, m_calluuid, m_dvpcalldirection, m_brigeuuid, m_switchname, m_sipfromuser, m_siptouser, m_callercontext, m_hanupcause, m_createdtimedimid, m_createdtime, m_duration, m_bridgedtime, m_hanguptime, m_appid, m_companyid, m_tenantid, m_extradata, m_isqueued, m_businessunit, m_agentanswered, m_callhangupdirectiona, m_callhangupdirectionb, m_progressec, m_flowbillsec, m_progressmediasec, m_waitsec, m_holdsec, m_billsec, m_queuesec, m_answersec, m_answeredtime, m_agentskill, m_objtype, m_objcategory, m_receivedby, m_outleg_answered)
     LEFT JOIN "DimDate" c ON a.m_createdtimedimid::text = c.datedimid::text
     INNER JOIN "DimCompany" d ON a.m_companyid = d.companyid AND a.m_tenantid = d.tenantid
  GROUP BY a.m_calluuid, a.m_recordinguuid, a.m_dvpcalldirection, a.m_brigeuuid, c.fulldatesl, c.year, c.month, c.monthname, c.dayofmonth, c.quarter, c.quartername, c.isweekday, a.m_switchname, a.m_sipfromuser, a.m_siptouser, a.m_callercontext, a.m_hanupcause, a.m_createdtime, a.m_duration, a.m_bridgedtime, a.m_hanguptime, a.m_appid, a.m_companyid, a.m_tenantid, a.m_extradata, a.m_isqueued, a.m_businessunit, a.m_agentanswered, a.m_callhangupdirectiona, a.m_callhangupdirectionb, a.m_progressec, a.m_flowbillsec, a.m_progressmediasec, 
     a.m_waitsec, a.m_holdsec, a.m_billsec, a.m_queuesec, a.m_answersec, a.m_answeredtime, a.m_agentskill, 
     a.m_objtype, a.m_objcategory, a.m_receivedby)
     
     LOOP
        dvpcalldirection :=  var_r.dvpcalldirection;
		sipfromuser :=  var_r.sipfromuser;
		siptouser :=  var_r.siptouser;
		receivedby :=  var_r.receivedby;
		agentskill :=  var_r.agentskill;
		agentanswered :=  var_r.agentanswered;
		createdtime :=  var_r.createdtime;
		fulldatesl :=  var_r.fulldatesl;
		year :=  var_r.year;
		month :=  var_r.month;
		monthname :=  var_r.monthname;
		dayofmonth :=  var_r.dayofmonth;
		quarter :=  var_r.quarter;
		quartername :=  var_r.quartername;
		isweekday :=  var_r.isweekday;
		duration :=  var_r.duration;
		billsec :=  var_r.billsec;
		waitsec :=  var_r.waitsec;
		queuesec :=  var_r.queuesec;
		holdsec :=  var_r.holdsec;
		objtype :=  var_r.objtype;
		objcategory :=  var_r.objcategory;
		businessunit :=  var_r.businessunit;

        RETURN NEXT;  
        END LOOP;
   END;

$function$;

CREATE OR REPLACE FUNCTION public.get_call_details(m_time_range integer, m_is_processed_table boolean)
 RETURNS SETOF call_record
 LANGUAGE plpgsql
AS $function$

declare
 temprow record;
 m_final_query text := 'SELECT ';
 m_final_result call_record;
 m_uuid varchar;
 m_recording_uuid varchar;
 m_sipto_user varchar;
 p_call_hangupDirection_a varchar;
 s_call_hangupDirection_b varchar;
 m_is_answered bool := 'false';
 m_hold_sec integer := 0;
 m_bill_sec integer := 0;
 m_progess_sec integer := 0;
 m_wait_sec integer := 0;
 m_progress_media_sec integer := 0;
 m_flow_bill_sec integer := 0;
 m_answer_sec integer;
 m_answered_time varchar; 
 m_obj_type varchar;
 m_obj_category varchar;
 m_received_by varchar;
 m_outleg_answered bool := 'false';
 m_outbound_leg bool := 'false';
 m_outbound_uuid varchar;
 m_legs_to_delete varchar[];
 
BEGIN
	FOR temprow IN
        SELECT * from "DimCallTemp" d join "FactCallTemp" f 
        on d.uuid=f.uuid 
        where direction = 'inbound' AND createdtime :: timestamp <= now() AT TIME ZONE 'Asia/Colombo' - INTERVAL '1 HOUR' * m_time_range
          and  calluuid = '963da0ab-7e51-4173-90a8-dfc9016cea49'
          
    LOOP
    	IF(temprow.dvpcalldirection IS NOT NULL) THEN 
			p_call_hangupDirection_a = temprow.hangupdisposition;
		ELSE
			p_call_hangupDirection_a = '-';
		END IF;	
		
		m_obj_type = temprow.objtype;
		m_obj_category = temprow.objcategory;
		m_hold_sec = COALESCE(temprow.holdsec,0);
		m_recording_uuid = temprow.uuid;
		m_uuid = temprow.uuid;
        
        SELECT  a.m_secondaryleg, a.m_legstodelete
   				FROM get_outbound_uuid(m_uuid, m_is_processed_table)  
                a(m_secondaryleg, m_legstodelete)
                INTO m_outbound_uuid, m_legs_to_delete;
        
 		SELECT 
        CASE WHEN dvpcalldirection IS NOT NULL THEN hangupdisposition ELSE '-' END, 
        COALESCE(siptouser, '-'), 
        COALESCE(answeredtime,'-'), 
        COALESCE (CASE WHEN holdsec is NOT NULL THEN holdsec+m_hold_sec ELSE m_hold_sec END,0),
        COALESCE(billsec,0), 
        CASE WHEN dvpcalldirection = 'outbound' THEN f.uuid ELSE m_recording_uuid END,
        CASE WHEN m_obj_type IS NULL THEN objtype ELSE m_obj_type END,
        CASE WHEN m_obj_category IS NULL THEN objcategory ELSE m_obj_category END,
        CASE WHEN COALESCE(billsec,0) > 0 THEN 'true' ELSE 'false' END,
        CASE WHEN COALESCE(billsec,0) <= 0 AND siptouser IS NOT NULL THEN duration ELSE answersec END,
        'true'
        INTO s_call_hangupDirection_b,m_received_by, m_answered_time, m_hold_sec, m_bill_sec, 
        m_recording_uuid, m_obj_type, m_obj_category, m_outleg_answered, m_answer_sec, m_outbound_leg
        FROM "DimCallTemp" d join "FactCallTemp" f 
        on d.uuid=f.uuid 
        WHERE f.calluuid=temprow.calluuid and d.direction = 'outbound' and createdtime :: timestamp <= now() AT TIME ZONE 'Asia/Colombo' - INTERVAL '1 HOUR' * m_time_range;      
                
        IF m_uuid IS NULL AND m_outbound_leg IS NOT NULL THEN
        CONTINUE;
        END IF;
        raise notice 'before if else % ', m_uuid;
   		IF m_outbound_leg IS NOT null then
   			   raise notice 'inside if % ', m_uuid;

   			m_final_query = m_final_query || 'd.uuid, 
            '''||m_recording_uuid||''' as recordinguuid, 
            f.calluuid, 
            COALESCE(d.dvpcalldirection, ''-''),
            COALESCE(f.bridgeuuid,''-''), 
            COALESCE(d.switchname,''-''), 
            COALESCE(d.sipfromuser,''-''),  
            COALESCE(d.siptouser,''-''),
            COALESCE(d.callercontext,''-''),  
            COALESCE(d.hangupcause,''-''), 
            COALESCE(f.createddatedimid,''-''),
            COALESCE(f.createdtime,''-''),  
            COALESCE(d.duration,0),
            COALESCE(f.bridgedtime,''-''), 
            COALESCE(f.hanguptime,''-''), 
            COALESCE(d.appid,0),  
            COALESCE(f.companyid,0),  
            COALESCE(f.tenantid,0),  
            COALESCE(d.extradata,''-''), 
            COALESCE(d.isqueued, false),  
            COALESCE(d.businessunit,''-''),  
            COALESCE(d.agentanswered, false), 
            '''||p_call_hangupDirection_a||''' as callhangupdirectiona,
            '''||s_call_hangupDirection_b||''' as callhangupdirectionb,
            COALESCE(f.progresssec,0),  
            COALESCE(f.flowbillsec,0), 
            COALESCE(f.progressmediasec,0), 
            COALESCE(f.waitsec,0), 
            '''||m_hold_sec||''' as holdsec, 
            '''||m_bill_sec||''' as billsec, 
            COALESCE(f.queuesec,0), 
            COALESCE('''||m_answer_sec||''',0) as answersec, 
            '''||m_answered_time||''' as answeredtime,  
            COALESCE(d.agentskill,''-''), 
            '''||m_obj_type||''' as objtype, 
            '''||m_obj_category||''' as objcategory, 
            '''||m_received_by||''' as receivedby,
            '''||m_outleg_answered||''' as outleganswered
            from "DimCallTemp" d join "FactCallTemp" f 
            on d.uuid=f.uuid 
            where f.calluuid= '''||temprow.calluuid||''' and direction = ''inbound''';

        else
           			   raise notice 'inside else % ', m_uuid;
	raise notice 'temprow.calluuid is % ', temprow.calluuid;
	raise notice 'temprow.calluuid printed above, %', p_call_hangupDirection_a;
   			m_final_query = m_final_query || 'd.uuid, 
            d.uuid, 
	        f.calluuid, 
            COALESCE(d.dvpcalldirection, ''-''),  
            COALESCE(f.bridgeuuid,''-''), 
            COALESCE(d.switchname,''-''), 
            COALESCE(d.sipfromuser,''-''), 
            COALESCE(d.siptouser,''-''), 
            COALESCE(d.callercontext,''-''), 
            COALESCE(d.hangupcause,''-''), 
            COALESCE(f.createddatedimid,''-''),
            COALESCE(f.createdtime,''-''), 
            COALESCE(d.duration,0), 
            COALESCE(f.bridgedtime,''-''), 
            COALESCE(f.hanguptime,''-''),
            COALESCE(d.appid,0), 
            COALESCE(f.companyid,0), 
            COALESCE(f.tenantid,0),
            COALESCE(d.extradata,''-'') , 
            COALESCE(d.isqueued,false), 
            COALESCE(d.businessunit,''-''), 
            COALESCE(d.agentanswered,false),
            '''||p_call_hangupDirection_a||''' as callhangupdirectiona,
            ''-'' as callhangupdirectionb,
            COALESCE(f.progresssec,0), 
            COALESCE(f.flowbillsec,0), 
            COALESCE(f.progressmediasec,0), 
            COALESCE(f.waitsec,0), 
            COALESCE(f.holdsec,0) as holdsec, 
            COALESCE(f.billsec,0), 
            COALESCE(f.queuesec,0),
            COALESCE(f.answersec,0), 
            COALESCE(f.answeredtime,''-''),
            COALESCE(d.agentskill,''-''), 
            COALESCE(d.objtype,''-''), 
            COALESCE(d.objcategory,''-''), 
            ''-'' as receivedby,
            FALSE as outleganswered
            from "DimCallTemp" d join "FactCallTemp" f 
            on d.uuid=f.uuid 
            where f.calluuid= '''||temprow.calluuid||''' and direction = ''inbound''';
   		END IF;
        
   raise notice 'm_final_query: % ', m_final_query;
--		if m_final_query is null then
--			continue;
--		end if;
            
   		EXECUTE m_final_query 
        INTO m_final_result.m_uuid, m_final_result.m_recordinguuid, 
        m_final_result.m_calluuid, m_final_result.m_dvpcalldirection, m_final_result.m_brigeuuid, m_final_result.m_switchname,
        m_final_result.m_sipfromuser, m_final_result.m_siptouser, m_final_result.m_callercontext, 
        m_final_result.m_hanupcause, m_final_result.m_createdtimedimid,m_final_result.m_createdtime, 
        m_final_result.m_duration,  m_final_result.m_bridgedtime, m_final_result.m_hanguptime,
        m_final_result.m_appid, m_final_result.m_companyid, m_final_result.m_tenantid, m_final_result.m_extradata, 
        m_final_result.m_isqueued, m_final_result.m_businessunit, m_final_result.m_agentanswered,
        m_final_result.m_callhangupdirectiona, m_final_result.m_callhangupdirectionb,
        m_final_result.m_progressec, m_final_result.m_flowbillsec, m_final_result.m_progressmediasec, 
        m_final_result.m_waitsec,m_final_result.m_holdsec, m_final_result.m_billsec, m_final_result.m_queuesec,
        m_final_result.m_answersec, m_final_result.m_answeredtime, m_final_result.m_agentskill, 
        m_final_result.m_objtype, m_final_result.m_objcategory, m_final_result.m_receivedby,
        m_final_result.m_outleganswered;
       
           IF m_is_processed_table THEN
--        raise notice 'm_legs_to_delete : %', m_legs_to_delete;
        	DELETE FROM "DimCallTemp"
 			WHERE uuid = ANY(m_legs_to_delete);
            DELETE FROM "FactCallTemp"
 			WHERE uuid = ANY(m_legs_to_delete);
            DELETE FROM "DimOriginatedLegs"
 			WHERE uuid = ANY(m_legs_to_delete);
        END IF;
   m_final_query := 'SELECT ';
   RAISE NOTICE 'rreerrrrrr%', m_final_result; 
   RETURN NEXT m_final_result;
   END LOOP;
END;

$function$;

CREATE OR REPLACE FUNCTION public.get_company(username character varying)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
	begin 
		return (select compid from "UserCompanyMapping" where user_name = username);
	end
	$function$;

CREATE OR REPLACE FUNCTION public.get_function_fields(fn_name character varying)
 RETURNS TABLE(fieldname character varying, fieldtype character varying)
 LANGUAGE plpgsql
AS $function$ 

BEGIN
	DROP TABLE if exists temp_call_det_0;
	EXECUTE 'create TEMP table if not exists temp_call_det_0 as select * from ' || fn_name::regproc || '() limit 1';
  
	RETURN QUERY SELECT 
		attname::varchar as fieldname,
		format_type(
			atttypid,
			atttypmod
		)::varchar as fieldtype
		from
			pg_attribute
		where
			attrelid = 'temp_call_det_0'::regclass
			and attnum > 0;
		
		DROP TABLE temp_call_det_0;
END; 

$function$;

CREATE OR REPLACE FUNCTION public.get_outbound_uuid(m_inbound_uuid character varying, m_is_processed_table boolean)
 RETURNS toget_outbound_type
 LANGUAGE plpgsql
AS $function$

declare
 result_record toget_outbound_type;
 temprow record;
 m_inbound_call_uuid varchar;
 m_final_result call_record;
 m_uuid varchar[];
 m_inbound_originated_leg varchar;
 m_sipto_user varchar;
 p_call_hangupDirection_a varchar;
 s_call_hangupDirection_b varchar;
 m_is_answered bool := 'false';
 m_hold_sec integer := 0;
 m_bill_sec integer := 0;
 m_progess_sec integer := 0;
 m_wait_sec integer := 0;
 m_progress_media_sec integer := 0;
 m_flow_bill_sec integer := 0;
 m_answer_sec integer;
 m_answered_time varchar; 
 m_obj_type varchar;
 m_obj_category varchar;
 m_array_uuid varchar;
 m_is_transfered bool := true;
 m_temp_is_transfered bool := true;
 m_temp_calluuid_array varchar;
 m_temp_value_array varchar[];
 m_temp_originated_legs_set varchar[];
 m_array_count integer := 0;
 m_legs_to_delete varchar[];

BEGIN
	SELECT COALESCE(calluuid,'-'),COALESCE(originated_leg,'-') into m_inbound_call_uuid,m_inbound_originated_leg 
    from "DimOriginatedLegs" 
    where uuid = m_inbound_uuid and direction = 'inbound' ;
    
    m_legs_to_delete[0] = m_inbound_uuid;
	
    IF m_inbound_call_uuid = '-' THEN
    	return m_final_result;
    END IF;
    
	m_inbound_originated_leg = substring(m_inbound_originated_leg from 1 for char_length(m_inbound_originated_leg)-1);	-- Remove unwanted end comma 
    -- regexp_replace(m_inbound_originated_leg,',\M','');
   
   	m_temp_calluuid_array = m_inbound_call_uuid || '@' || m_inbound_originated_leg;
    m_uuid = array_append(m_uuid, m_temp_calluuid_array);
	
	-- raise notice 'm_temp_calluuid_array : %', m_temp_calluuid_array;
	raise notice 'm_uuid : %', array_length(m_uuid,1);
	
    WHILE array_length(m_uuid,1) > m_array_count AND m_array_count < 5  
    -- FOREACH m_array_uuid IN ARRAY m_uuid 
	LOOP
	
    m_array_count = m_array_count + 1;
	
	m_array_uuid = m_uuid[m_array_count];
	
    m_temp_value_array = regexp_split_to_array(m_array_uuid,'@');
	
	-- raise notice 'm_temp_value_array[2] : %', m_temp_value_array[2];
	
	m_temp_originated_legs_set = string_to_array(m_temp_value_array[2],',');
	 
	-- m_temp_originated_legs_set = regexp_replace(m_temp_value_array[2],',',''',''','g');
	 
	raise notice 'm_temp_originated_legs_set : %', m_temp_originated_legs_set;
	 
	-- m_temp_originated_legs_set = '''' || m_temp_originated_legs_set || '''';
	 
	 -- raise notice 'm_temp_originated_legs_set : %', m_temp_originated_legs_set;
	 
    raise notice 'array current element : % and row no : %',m_array_uuid, m_array_count;
    
		FOR temprow IN
        SELECT * from "DimOriginatedLegs" 
        where (calluuid = m_temp_value_array[1] OR uuid = ANY (m_temp_originated_legs_set)) AND  direction = 'outbound' 
    	LOOP
			raise notice 'temprow : %', temprow.uuid;
			raise notice 'temprow.istransferleg : %', temprow.istransferleg;
			
        	IF temprow.istransferleg THEN 
            	m_temp_calluuid_array = temprow.calluuid || '@' || substring(temprow.originated_leg from 1 for char_length(temprow.originated_leg)-1);
    			m_uuid = array_append(m_uuid, m_temp_calluuid_array);
                raise notice 'inbound: % and outbound: %', m_inbound_uuid, temprow.uuid;
            ELSE
            	SELECT temprow.uuid INTO result_record.m_secondaryleg;
                raise notice 'inbound: % and outbound: %', m_inbound_uuid, temprow.uuid;
            END IF; 
            
            IF m_is_processed_table THEN
                m_legs_to_delete = array_append(m_legs_to_delete,  temprow.uuid);  
                raise notice 'm_legs_to_delete : %', m_legs_to_delete;	
            END IF; 
        END LOOP; -- m_array_count = m_array_count + 1;   
        
    END LOOP;    
  --  ret := array_append(m_legs_to_delete, m_inbound_uuid);
    SELECT m_legs_to_delete INTO result_record.m_legstodelete;
   -- raise notice 'ret : %', ret;	
    
    RETURN result_record;
    
END;

$function$;

CREATE OR REPLACE FUNCTION public.get_tenant(username character varying)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
	begin 
		return (select tenantid from "UserCompanyMapping" where user_name = username);
	end
	$function$;

CREATE OR REPLACE FUNCTION public.insert_scheduled_data(m_hours integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$

BEGIN
    INSERT INTO "ProcessedCalls" (uuid,recordinguuid, calluuid, dvpcalldirection, brigeuuid, switchname,
                                          sipfromuser, siptouser, callercontext, hanupcause, createdtimedimid, 
                                         createdtime, duration, bridgedtime, hanguptime,appid, companyid,tenantid, 
                                         extradata, isqueued, businessunit, agentanswered, callhangupdirectiona, 
                                         callhangupdirectionb, progressec, flowbillsec, progressmediasec, waitsec,
                                         holdsec, billsec, queuesec, answersec, answeredtime, agentskill, objtype,
                                         objcategory, receivedby, outleganswered)
			
            SELECT distinct m_uuid, m_recordinguuid, m_calluuid, m_dvpcalldirection, m_brigeuuid, m_switchname, 
                     m_sipfromuser, m_siptouser, m_callercontext, m_hanupcause, m_createdtimedimid, m_createdtime, 
                     m_duration, m_bridgedtime, m_hanguptime, m_appid, m_companyid, m_tenantid, m_extradata, 
                     m_isqueued, m_businessunit, m_agentanswered, m_callhangupdirectiona, m_callhangupdirectionb, 
                     m_progressec, m_flowbillsec, m_progressmediasec, m_waitsec, m_holdsec, m_billsec, m_queuesec, 
                     m_answersec, m_answeredtime, m_agentskill, m_objtype, m_objcategory, m_receivedby, 
                     m_outleganswered
			FROM public.get_call_details(m_hours, true);
END;

$function$;

CREATE OR REPLACE FUNCTION public.totalrecords()
 RETURNS integer
 LANGUAGE plpgsql
AS $function$

declare
	total integer;
BEGIN
   SELECT count(*) into total FROM "FactCall";
   RETURN total;
END;

$function$;

CREATE OR REPLACE FUNCTION public.valid_user(username character varying)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$ 
           DECLARE 
--                ts_promoter_id BIGINT = substring(current_setting('ts.promoter',TRUE),26)::BIGINT; 
           BEGIN 
                RETURN EXISTS( 
                select true from (select * from newtable)a
 inner join (select * from ucmap where user_name = username)b 
 on a.compid = b.compid and a.tenantid = b.tenantid
);
           END 
        $function$;

