CREATE SCHEMA Security;

CREATE FUNCTION Security.fn_securitypredicate(@region AS VARCHAR(50))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS fn_securitypredicate_result
WHERE @region = USER_NAME();  -- or use SUSER_SNAME() or logic from a mapping table


CREATE SECURITY POLICY RegionFilter1
ADD FILTER PREDICATE Security.fn_securitypredicate(region)
ON dbo.ordersdata_managed_INC_dest
WITH (STATE = ON);

EXECUTE AS USER = 'East';
select * from dbo.ordersdata_managed_INC_dest;
REVERT;

ALTER TABLE dbo.ordersdata_managed_INC_dest
ALTER COLUMN total_amount ADD MASKED WITH (FUNCTION = 'default()');

GRANT UNMASK TO [East];
REVOKE UNMASK TO [East];

Alter VIEW dbo.vw_orders_limited AS
SELECT order_id, product_id, quantity,region
FROM dbo.ordersdata_managed_INC_dest;

GRANT SELECT ON dbo.vw_orders_limited TO East;
GRANT SELECT ON SCHEMA::dbo TO East;


EXECUTE AS USER = 'East';
select * from dbo.vw_orders_limited;
REVERT;

select * from dbo.vw_orders_limited 
--Role-Based Access Control (RBAC)

az role assignment create --assignee <userPrincipalName> --role "Synapse Contributor" --scope <workspaceScope>


-- Create role
CREATE ROLE region;

-- Grant SELECT permission on a table or view
GRANT SELECT ON dbo.ordersdata_managed_INC_dest TO region;

-- Add user to role
EXEC sp_addrolemember 'region', 'west'; 


EXECUTE AS USER = 'west';
select * from dbo.ordersdata_managed_INC_dest ;
REVERT;

-- Grant SELECT only on the limited view
GRANT SELECT ON dbo.vw_customers_limited TO sales_user;

GRANT SELECT, INSERT ON dbo.ordersdata_managed_INC_dest TO [East];

create user west without LOGIN;
