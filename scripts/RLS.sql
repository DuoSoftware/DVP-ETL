CREATE USER <company_name>;
ALTER USER <company_name> password <password>;
GRANT ALL PRIVILEGES ON DATABASE "DiginToFacetone" TO <company_name>;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO <company_name>;

CREATE OR REPLACE FUNCTION get_tenant(username CHARACTER VARYING)
RETURNS integer
	LANGUAGE plpgsql
	AS $$
	BEGIN
		RETURN (SELECT tenantid FROM "UserCompanyMapping" WHERE user_name = username);
	END
	$$

-- SELECT * FROM get_tenant(current_user::text)

CREATE OR REPLACE FUNCTION get_company(username CHARACTER VARYING)
RETURNS integer
	LANGUAGE plpgsql
	AS $$
	BEGIN
		RETURN (SELECT compid FROM "UserCompanyMapping" WHERE user_name = username);
	END
	$$

-- SELECT * FROM get_company(current_user::text)

CREATE POLICY Digin_policy
    ON public."DimCompany" TO <company_name>
    USING ( companyid = get_company(current_user::text) AND tenantid = get_tenant(current_user::text));

ALTER TABLE public."DimCompany" ENABLE ROW LEVEL SECURITY;
