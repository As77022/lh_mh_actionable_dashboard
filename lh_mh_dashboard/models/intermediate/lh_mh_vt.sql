--VT
SELECT distinct vt."VTId", vt."FullName" as VTName, vt."Mobile" as VTMobile 
FROM "stg_lh_mh"."VocationalTrainers" AS vt
WHERE vt."IsActive" = '1' AND vt."DateOfResignation" IS null