-- Drop table

-- DROP TABLE public."DimCall"

CREATE TABLE public."DimCall" (
	id serial NOT NULL,
	uuid varchar(255) NOT NULL,
	sipfromuser varchar(255) NULL,
	siptouser varchar(255) NULL,
	hangupcause varchar(255) NULL,
	hangupdisposition varchar(255) NULL,
	direction varchar(255) NULL,
	switchname varchar(255) NULL,
	callercontext varchar(255) NULL,
	isanswered bool NULL,
	duration int4 NULL,
	agentanswered bool NULL,
	originatedlegs varchar(1000) NULL,
	istransferleg bool NULL,
	extradata varchar(1000) NULL,
	dvpcalldirection varchar(255) NULL,
	objclass varchar(255) NULL,
	objtype varchar(255) NULL,
	objcategory varchar(255) NULL,
	appid int4 NULL,
	agentskill varchar(255) NULL,
	isqueued bool NULL,
	sipresource varchar(255) NULL,
	campaignid int4 NULL,
	campaignname varchar(1000) NULL,
	businessunit varchar(255) NULL,
	CONSTRAINT "DimCall_pkey" PRIMARY KEY (uuid)
)
WITH (
	OIDS=FALSE
) ;

-- Drop table

-- DROP TABLE public."DimCallTemp"

CREATE TABLE public."DimCallTemp" (
	id serial NOT NULL,
	uuid varchar(255) NOT NULL,
	sipfromuser varchar(255) NULL,
	siptouser varchar(255) NULL,
	hangupcause varchar(255) NULL,
	hangupdisposition varchar(255) NULL,
	direction varchar(255) NULL,
	switchname varchar(255) NULL,
	callercontext varchar(255) NULL,
	isanswered bool NULL,
	duration int4 NULL,
	agentanswered bool NULL,
	originatedlegs varchar(1000) NULL,
	istransferleg bool NULL,
	extradata varchar(1000) NULL,
	dvpcalldirection varchar(255) NULL,
	objclass varchar(255) NULL,
	objtype varchar(255) NULL,
	objcategory varchar(255) NULL,
	appid int4 NULL,
	agentskill varchar(255) NULL,
	isqueued bool NULL,
	sipresource varchar(255) NULL,
	campaignid int4 NULL,
	campaignname varchar(1000) NULL,
	businessunit varchar(255) NULL,
	CONSTRAINT "DimCallTemp_pkey" PRIMARY KEY (uuid)
)
WITH (
	OIDS=FALSE
) ;

-- Drop table

-- DROP TABLE public."DimCompany"

CREATE TABLE public."DimCompany" (
	id serial NOT NULL,
	companyid int4 NOT NULL,
	tenantid int4 NULL,
	companyname varchar(255) NULL,
	companyenabled bool NULL,
	created_at date NULL,
	updated_at date NULL,
	CONSTRAINT "DimCompany_pkey" PRIMARY KEY (companyid)
)
WITH (
	OIDS=FALSE
) ;

-- Drop table

-- DROP TABLE public."DimDate"

CREATE TABLE public."DimDate" (
	datedimid varchar(8) NOT NULL,
	"date" timestamp NULL,
	fulldatesl bpchar(10) NULL,
	fulldateusa bpchar(10) NULL,
	dayofmonth int4 NULL,
	daysuffix varchar(4) NULL,
	dayname varchar(9) NULL,
	dayofweekusa int4 NULL,
	dayofweeksl int4 NULL,
	dayofweekinmonth int4 NULL,
	dayofweekinyear int4 NULL,
	dayofquarter int4 NULL,
	dayofyear int4 NULL,
	weekofmonth int4 NULL,
	weekofquarter int4 NULL,
	weekofyear int4 NULL,
	"month" int4 NULL,
	monthname varchar(9) NULL,
	monthofquarter int4 NULL,
	quarter int4 NULL,
	quartername varchar(9) NULL,
	"year" int4 NULL,
	yearname bpchar(7) NULL,
	monthyear bpchar(10) NULL,
	mmyyyy bpchar(6) NULL,
	firstdayofmonth date NULL,
	lastdayofmonth date NULL,
	firstdayofquarter date NULL,
	lastdayofquarter date NULL,
	firstdayofyear date NULL,
	lastdayofyear date NULL,
	isholidayusa bool NULL,
	isweekday bool NULL,
	holidayusa varchar(50) NULL,
	isholidaysl bool NULL,
	holidaysl varchar(50) NULL,
	CONSTRAINT "DimDate_pkey" PRIMARY KEY (datedimid)
)
WITH (
	OIDS=FALSE
) ;

-- Drop table

-- DROP TABLE public."DimEngagement"

CREATE TABLE public."DimEngagement" (
	id serial NOT NULL,
	engagement_id varchar(255) NOT NULL,
	profile varchar(255) NULL,
	channel_from varchar(255) NULL,
	channel_to varchar(255) NULL,
	company int4 NULL,
	has_profile bool NULL,
	tenant int4 NULL,
	notes varchar(255) NULL,
	channel varchar(255) NULL,
	CONSTRAINT "DimEngagement_pkey" PRIMARY KEY (engagement_id)
)
WITH (
	OIDS=FALSE
) ;

-- Drop table

-- DROP TABLE public."DimExternalUsers"

CREATE TABLE public."DimExternalUsers" (
	id serial NOT NULL,
	external_user_id varchar(255) NOT NULL,
	title bpchar(5) NULL,
	"name" varchar(255) NULL,
	avatar varchar(255) NULL,
	birthday varchar(255) NULL,
	gender varchar(6) NULL,
	first_name varchar(255) NULL,
	last_name varchar(255) NULL,
	locale varchar(255) NULL,
	ssn varchar(255) NULL,
	password varchar(255) NULL,
	phone varchar(255) NULL,
	email varchar(255) NULL,
	facebook varchar(255) NULL,
	twitter varchar(255) NULL,
	linkedin varchar(255) NULL,
	googleplus varchar(255) NULL,
	skype varchar(255) NULL,
	thirdpartyreference varchar(255) NULL,
	landnumber varchar(255) NULL,
	company int8 NULL,
	tenant int8 NULL,
	custom_fields varchar(255) NULL,
	tags varchar(255) NULL,
	contacts varchar(1000) NULL,
	zipcode varchar(255) NULL,
	address_number varchar(255) NULL,
	city varchar(255) NULL,
	province varchar(255) NULL,
	country varchar(255) NULL,
	CONSTRAINT "DimExternalUsers_id_key" UNIQUE (id)
)
WITH (
	OIDS=FALSE
) ;

-- Drop table

-- DROP TABLE public."DimOriginatedLegs"

CREATE TABLE public."DimOriginatedLegs" (
	id serial NOT NULL,
	uuid varchar(255) NOT NULL,
	calluuid varchar(255) NULL,
	direction varchar(255) NULL,
	originated_leg varchar(255) NULL,
	memberuuid varchar(255) NULL,
	objtype varchar(255) NULL,
	istransferleg bool NULL,
	CONSTRAINT "DimOriginatedLegs_id_key" UNIQUE (id)
)
WITH (
	OIDS=FALSE
) ;

-- Drop table

-- DROP TABLE public."DimOriginatedLegsTemp"

CREATE TABLE public."DimOriginatedLegsTemp" (
	id serial NOT NULL,
	uuid varchar(255) NOT NULL,
	calluuid varchar(255) NULL,
	direction varchar(255) NULL,
	originated_leg varchar(255) NULL,
	memberuuid varchar(255) NULL,
	objtype varchar(255) NULL,
	istransferleg bool NULL,
	CONSTRAINT "DimOriginatedLegsTemp_id_key" UNIQUE (id)
)
WITH (
	OIDS=FALSE
) ;

-- Drop table

-- DROP TABLE public."DimTenant"

CREATE TABLE public."DimTenant" (
	id serial NOT NULL,
	tenantid int4 NULL,
	rootname varchar(255) NULL,
	created_at date NULL,
	updated_at date NULL
)
WITH (
	OIDS=FALSE
) ;

-- Drop table

-- DROP TABLE public."DimTicket"

CREATE TABLE public."DimTicket" (
	id serial NOT NULL,
	ticket_id varchar(255) NOT NULL,
	active bool NULL,
	tid int8 NULL,
	is_sub_ticket bool NULL,
	subject varchar(255) NULL,
	reference varchar(255) NULL,
	description varchar(2000) NULL,
	priority varchar(255) NULL,
	ticket_type varchar(255) NULL,
	status_type varchar(255) NULL,
	channel varchar(255) NULL,
	company int8 NULL,
	tenant int8 NULL,
	assignee varchar(255) NULL,
	assignee_group varchar(255) NULL,
	requester varchar(255) NULL,
	submitter varchar(255) NULL,
	collaborators text[] NULL,
	watchers text[] NULL,
	attachments varchar(255) NULL,
	sub_tickets text[] NULL,
	related_tickets text[] NULL,
	merged_tickets text[] NULL,
	isolated_tags text[] NULL,
	sla varchar(255) NULL,
	comments text[] NULL,
	event_author varchar(255) NULL,
	security_level int8 NULL,
	tags text[] NULL
)
WITH (
	OIDS=FALSE
) ;

-- Drop table

-- DROP TABLE public."FactCall"

CREATE TABLE public."FactCall" (
	id serial NOT NULL,
	uuid varchar(255) NOT NULL,
	calluuid varchar(255) NULL,
	bridgeuuid varchar(255) NULL,
	createddatedimid varchar(255) NULL,
	answereddatedimid varchar(255) NULL,
	bridgeddatedimid varchar(255) NULL,
	hangupdatedimid varchar(255) NULL,
	createdtimedimid varchar(255) NULL,
	answeredtimedimid varchar(255) NULL,
	bridgedtimedimid varchar(255) NULL,
	hanguptimedimid varchar(255) NULL,
	createdtime varchar(255) NULL,
	answeredtime varchar(255) NULL,
	bridgedtime varchar(255) NULL,
	hanguptime varchar(255) NULL,
	billsec int4 NULL,
	holdsec int4 NULL,
	queuesec int4 NULL,
	progresssec int4 NULL,
	answersec int4 NULL,
	waitsec int4 NULL,
	progressmediasec int4 NULL,
	flowbillsec int4 NULL,
	tenantid int4 NULL,
	companyid int4 NULL,
	CONSTRAINT "FactCall_uuid_key" UNIQUE (uuid)
)
WITH (
	OIDS=FALSE
) ;

-- Drop table

-- DROP TABLE public."FactCallTemp"

CREATE TABLE public."FactCallTemp" (
	id serial NOT NULL,
	uuid varchar(255) NOT NULL,
	calluuid varchar(255) NULL,
	bridgeuuid varchar(255) NULL,
	createddatedimid varchar(255) NULL,
	answereddatedimid varchar(255) NULL,
	bridgeddatedimid varchar(255) NULL,
	hangupdatedimid varchar(255) NULL,
	createdtimedimid varchar(255) NULL,
	answeredtimedimid varchar(255) NULL,
	bridgedtimedimid varchar(255) NULL,
	hanguptimedimid varchar(255) NULL,
	createdtime varchar(255) NULL,
	answeredtime varchar(255) NULL,
	bridgedtime varchar(255) NULL,
	hanguptime varchar(255) NULL,
	billsec int4 NULL,
	holdsec int4 NULL,
	queuesec int4 NULL,
	progresssec int4 NULL,
	answersec int4 NULL,
	waitsec int4 NULL,
	progressmediasec int4 NULL,
	flowbillsec int4 NULL,
	companyid int4 NULL,
	tenantid int4 NULL,
	CONSTRAINT "FactCallTemp_uuid_key" UNIQUE (uuid)
)
WITH (
	OIDS=FALSE
) ;

-- Drop table

-- DROP TABLE public."FactEngagement"

CREATE TABLE public."FactEngagement" (
	id serial NOT NULL,
	engagement_id varchar(255) NOT NULL,
	created_at_dim_id varchar(255) NULL,
	updated_at_dim_id varchar(255) NULL,
	created_time_dim_id varchar(255) NULL,
	updated_time_dim_id varchar(255) NULL,
	created_at varchar(255) NULL,
	updated_at varchar(255) NULL,
	CONSTRAINT "FactEngagement_engagement_id_key" UNIQUE (engagement_id)
)
WITH (
	OIDS=FALSE
) ;

-- Drop table

-- DROP TABLE public."FactExternalUsers"

CREATE TABLE public."FactExternalUsers" (
	id serial NOT NULL,
	external_user_id varchar(255) NOT NULL,
	created_at_dim_id varchar(255) NULL,
	updated_at_dim_id varchar(255) NULL,
	created_time_dim_id varchar(255) NULL,
	updated_time_dim_id varchar(255) NULL,
	created_at varchar(255) NULL,
	updated_at varchar(255) NULL,
	CONSTRAINT "FactExternalUsers_id_key" UNIQUE (id)
)
WITH (
	OIDS=FALSE
) ;

-- Drop table

-- DROP TABLE public."FactTicket"

CREATE TABLE public."FactTicket" (
	id serial NOT NULL,
	ticket_id varchar(255) NOT NULL,
	engagement_session_id varchar(255) NULL,
	created_at varchar(255) NULL,
	created_at_dim_id varchar(255) NULL,
	created_time_dim_id varchar(255) NULL,
	updated_at varchar(255) NULL,
	updated_at_dim_id varchar(255) NULL,
	updated_time_dim_id varchar(255) NULL,
	due_at varchar(255) NULL,
	due_at_dim_id varchar(255) NULL,
	due_time_dim_id varchar(255) NULL,
	time_estimation int8 NULL,
	re_opens varchar(255) NULL,
	waited_time int8 NULL,
	worked_time int8 NULL,
	resolution_time int8 NULL,
	CONSTRAINT "FactTicket_id_key" UNIQUE (id)
)
WITH (
	OIDS=FALSE
) ;

-- Drop table

-- DROP TABLE public."LegsToDelete"

CREATE TABLE public."LegsToDelete" (
	uuid_list varchar(255) NULL
)
WITH (
	OIDS=FALSE
) ;

-- Drop table

-- DROP TABLE public."ProcessedCalls"

CREATE TABLE public."ProcessedCalls" (
	id serial NOT NULL,
	uuid varchar(255) NOT NULL,
	recordinguuid varchar(255) NULL,
	calluuid varchar(255) NULL,
	dvpcalldirection varchar(255) NULL,
	brigeuuid varchar(255) NULL,
	switchname varchar(255) NULL,
	sipfromuser varchar(255) NULL,
	siptouser varchar(255) NULL,
	callercontext varchar(255) NULL,
	hanupcause varchar(255) NULL,
	createdtimedimid varchar(255) NULL,
	createdtime varchar(255) NULL,
	duration int4 NULL,
	bridgedtime varchar(255) NULL,
	hanguptime varchar(255) NULL,
	appid varchar(255) NULL,
	companyid int4 NULL,
	tenantid int4 NULL,
	extradata varchar(255) NULL,
	isqueued bool NULL,
	businessunit varchar(255) NULL,
	agentanswered bool NULL,
	callhangupdirectiona varchar(255) NULL,
	callhangupdirectionb varchar(255) NULL,
	progressec int4 NULL,
	flowbillsec int4 NULL,
	progressmediasec int4 NULL,
	waitsec int4 NULL,
	holdsec int4 NULL,
	billsec int4 NULL,
	queuesec int4 NULL,
	answersec int4 NULL,
	answeredtime varchar(255) NULL,
	agentskill varchar(255) NULL,
	objtype varchar(255) NULL,
	objcategory varchar(255) NULL,
	receivedby varchar(255) NULL,
	outleganswered bool NULL,
	CONSTRAINT "ProcessedCalls_uuid_key" UNIQUE (uuid)
)
WITH (
	OIDS=FALSE
) ;

-- Drop table

-- DROP TABLE public."UserCompanyMapping"

CREATE TABLE public."UserCompanyMapping" (
	user_name varchar NULL,
	compid int4 NULL,
	tenantid int4 NULL
)
WITH (
	OIDS=FALSE
) ;

-- Drop table

-- DROP TABLE public.ucmap

CREATE TABLE public.ucmap (
	user_name varchar NULL,
	compid varchar NULL,
	tenantid varchar NULL
)
WITH (
	OIDS=FALSE
) ;

