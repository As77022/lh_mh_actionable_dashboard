-- This table is same as connected in dashboard_mh (school)
WITH temp_table AS 
(
SELECT Distinct s."SchoolId", s."SchoolName", s."UDISE", s."SchoolTypeId" , 
scg."CategoryName" as SchoolCategory, "dv"."Name" 
FROM "stg_lh_mh"."Schools" AS s
INNER JOIN {{source('LH_MH_Dashboard',"SchoolCategories")}} AS scg on s."SchoolCategoryId" = scg."SchoolCategoryId"
LEFT JOIN  {{source('LH_MH_Dashboard',"DataValues")}} AS dv on s."SchoolTypeId" = dv."DataValueId" and dv."DataTypeId" = S."SchoolTypeId"
WHERE s."IsActive" = '1' 
)
SELECT * FROM temp_table