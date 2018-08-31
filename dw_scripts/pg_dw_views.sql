CREATE OR REPLACE VIEW public.external_users_view AS
 SELECT a.id,
    a.external_user_id,
    b.title,
    b.name,
    b.birthday,
    b.gender,
    b.first_name,
    b.last_name,
    b.locale,
    b.ssn,
    b.password,
    b.phone,
    b.email,
    b.landnumber,
    b.facebook,
    b.twitter,
    b.linkedin,
    b.googleplus,
    b.skype,
    b.thirdpartyreference,
    b.company,
    b.tenant,
    b.custom_fields,
    b.tags,
    b.contacts,
    b.zipcode,
    concat(b.address_number, b.city, b.province, b.country) AS address,
    c.fulldatesl AS createddate,
    d.fulldatesl AS lastupdateddate
   FROM "FactExternalUsers" a
     LEFT JOIN "DimExternalUsers" b ON a.external_user_id::text = b.external_user_id::text
     LEFT JOIN "DimDate" c ON a.created_at_dim_id::text = c.datedimid::text
     LEFT JOIN "DimDate" d ON a.updated_at_dim_id::text = d.datedimid::text
  GROUP BY a.id, a.external_user_id, b.title, b.name, b.birthday, b.gender, b.first_name, b.last_name, b.locale, b.ssn, b.password, b.phone, b.email, b.landnumber, b.facebook, b.twitter, b.linkedin, b.googleplus, b.skype, b.thirdpartyreference, b.company, b.tenant, b.custom_fields, b.tags, b.contacts, b.zipcode, (concat(b.address_number, b.city, b.province, b.country)), c.fulldatesl, d.fulldatesl, d.datedimid
  ORDER BY d.datedimid DESC;

CREATE OR REPLACE VIEW public.ticket_view AS
 SELECT a.id,
    a.ticket_id,
    b.reference,
    b.subject,
    c.phone AS phonenumber,
    c.email,
    c.ssn,
    c.first_name,
    c.last_name,
    concat(c.address_number, c.city, c.province, c.country) AS address,
    b.submitter AS fromnumber,
    d.fulldatesl AS createddate,
    max(e.fulldatesl) AS lastupdateddate,
    b.assignee,
    b.submitter,
    b.requester,
    b.channel,
    b.status_type,
    b.priority,
    b.sla AS sla_violated,
    b.description,
    b.comments,
    b.tags
   FROM "FactTicket" a
     LEFT JOIN "DimTicket" b ON a.ticket_id::text = b.ticket_id::text
     LEFT JOIN "DimExternalUsers" c ON b.requester::text = c.external_user_id::text
     LEFT JOIN "DimDate" d ON a.created_at_dim_id::text = d.datedimid::text
     LEFT JOIN "DimDate" e ON a.updated_at_dim_id::text = e.datedimid::text
  GROUP BY a.id, a.ticket_id, b.reference, b.subject, c.phone, c.email, c.ssn, c.first_name, c.last_name, (concat(c.address_number, c.city, c.province, c.country)), b.submitter, d.fulldatesl, b.assignee, b.requester, b.channel, b.status_type, b.priority, b.sla, b.description, b.comments, b.tags, e.datedimid
  ORDER BY e.datedimid DESC;

CREATE OR REPLACE VIEW public.updated_ext_users_view AS
 SELECT DISTINCT ON (a.external_user_id) a.external_user_id,
    a.id,
    b.title,
    b.name,
    b.birthday,
    b.gender,
    b.first_name,
    b.last_name,
    b.locale,
    b.ssn,
    b.password,
    b.phone,
    b.email,
    b.landnumber,
    b.facebook,
    b.twitter,
    b.linkedin,
    b.googleplus,
    b.skype,
    b.thirdpartyreference,
    b.company,
    b.tenant,
    b.custom_fields,
    b.tags,
    b.contacts,
    b.zipcode,
    concat(b.address_number, b.city, b.province, b.country) AS address,
    c.fulldatesl AS createddate,
    d.fulldatesl AS lastupdateddate
   FROM "FactExternalUsers" a
     LEFT JOIN "DimExternalUsers" b ON a.external_user_id::text = b.external_user_id::text
     LEFT JOIN "DimDate" c ON a.created_at_dim_id::text = c.datedimid::text
     LEFT JOIN "DimDate" d ON a.updated_at_dim_id::text = d.datedimid::text
  ORDER BY a.external_user_id DESC, (a.updated_at_dim_id::integer) DESC, (a.updated_time_dim_id::integer) DESC;

CREATE OR REPLACE VIEW public.updated_ticket_view AS
 SELECT DISTINCT ON (a.ticket_id) a.ticket_id,
    a.id,
    b.reference,
    a.updated_at_dim_id,
    a.updated_time_dim_id,
    b.subject,
    c.phone AS phonenumber,
    c.email,
    c.ssn,
    c.first_name,
    c.last_name,
    concat(c.address_number, c.city, c.province, c.country) AS address,
    b.submitter AS fromnumber,
    d.fulldatesl AS createddate,
    e.fulldatesl AS lastupdateddate,
    b.assignee,
    b.submitter,
    b.requester,
    b.channel,
    b.status_type,
    b.priority,
    b.sla AS sla_violated,
    b.description,
    b.comments,
    b.tags
   FROM "FactTicket" a
     LEFT JOIN "DimTicket" b ON a.ticket_id::text = b.ticket_id::text
     LEFT JOIN "DimExternalUsers" c ON b.requester::text = c.external_user_id::text
     LEFT JOIN "DimDate" d ON a.created_at_dim_id::text = d.datedimid::text
     LEFT JOIN "DimDate" e ON a.updated_at_dim_id::text = e.datedimid::text
  ORDER BY a.ticket_id DESC, (a.updated_at_dim_id::integer) DESC, (a.updated_time_dim_id::integer) DESC;

