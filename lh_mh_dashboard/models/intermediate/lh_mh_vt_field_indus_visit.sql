-- This table is same as connected in dashboard_mh IV
WITH "VTFieldIndustryVisitStatusData" AS 
(
    SELECT DISTINCT 
        "fvc"."AcademicYearId", 
        "fvc"."VTSchoolSectorId", 
        "fvc"."VTId", 
        "fvc"."ClassTaughtId",
        to_char("fvc"."ReportingDate", 'Month-YYYY') AS "MonthYear",
        --DATE_FORMAT("fvc"."ReportingDate", "%M-%Y") AS "MonthYear", 
        COUNT("fvc"."VTFieldIndustryVisitConductedId") AS "NoOfFVConducted"
    FROM {{source('LH_MH_Dashboard',"VTFieldIndustryVisitConducted")}} AS "fvc"
    INNER JOIN {{source('LH_MH_Dashboard',"VTSchoolSectors")}} AS "vss" ON "fvc"."VTSchoolSectorId" = "vss"."VTSchoolSectorId" AND "fvc"."VTId" = "vss"."VTId" AND "vss"."DateOfRemoval" IS NULL AND "vss"."IsActive" = '1'
    INNER JOIN {{source('LH_MH_Dashboard',"VCTrainersMap")}} AS "vtm" ON "vss"."AcademicYearId" = "vtm"."AcademicYearId" AND "vss"."VTId" = "vtm"."VTId" AND "vtm"."DateOfResignation" IS NULL AND "vtm"."IsActive" = '1'
    INNER JOIN {{source('LH_MH_Dashboard',"VocationalTrainers")}} AS "vt" ON "vtm"."VTId" = "vt"."VTId" AND "vt"."IsActive" = '1'  
    INNER JOIN {{source('LH_MH_Dashboard',"AcademicYears")}} AS "ay" ON "vss"."AcademicYearId" = "ay"."AcademicYearId" AND "ay"."IsActive" = '1'          
    INNER JOIN {{source('LH_MH_Dashboard',"Schools")}} AS "s" ON "vss"."SchoolId" = "s"."SchoolId" AND "s"."IsActive" = '1'		
    WHERE DATE("fvc"."ReportingDate") BETWEEN DATE("ay"."StartMonth") AND DATE("ay"."EndMonth")
    GROUP BY "fvc"."AcademicYearId", "fvc"."VTSchoolSectorId", "fvc"."ClassTaughtId","fvc"."VTId", "MonthYear"
) 
SELECT DISTINCT 
    "ay"."YearName" AS "AcademicYear",		
    "say"."YearName" AS "SchoolAllottedYear",
    "ph"."PhaseName",
    "vtp"."VTPName",
    "vt"."FullName" AS "VTName",
    "vt"."Mobile" AS "VTMobile",
    "vt"."Email" AS "VTEmail",	
    "vtm"."DateOfJoining" AS "VTDateOfJoining",
    "vc"."FullName" AS "VCName",
    "vc"."Mobile" AS "VCMobile",
    "vc"."EmailId" AS "VCEmail",
    "hm"."FullName" AS "HMName", 
    "hm"."Mobile" AS "HMMobile", 
    "hm"."Email" AS "HMEmail", 
    "dvs"."Name" AS "SchoolManagement",
    "dv"."DivisionName",
    "ds"."DistrictName",
    "s"."BlockName",
    "s"."UDISE",
    "s"."SchoolName",
    "se"."SectorName",
    "jr"."JobRoleName",
    "sc"."Name" AS "ClassName",        
    "fvc"."MonthYear",
    'NA' AS "FieldVisitStatus",
    "fvc"."NoOfFVConducted"
FROM "VTFieldIndustryVisitStatusData" AS "fvc"
INNER JOIN {{source('LH_MH_Dashboard',"VTSchoolSectors")}} AS "vss" ON "fvc"."AcademicYearId" = "vss"."AcademicYearId" AND "fvc"."VTSchoolSectorId" = "vss"."VTSchoolSectorId" AND "fvc"."VTId" = "vss"."VTId" AND "vss"."DateOfRemoval" IS NULL AND "vss"."IsActive" = '1'
INNER JOIN {{source('LH_MH_Dashboard',"VCTrainersMap")}} AS "vtm" ON "vss"."AcademicYearId" = "vtm"."AcademicYearId" AND "vss"."VTId" = "vtm"."VTId" AND "vtm"."DateOfResignation" IS NULL AND "vtm"."IsActive" = '1'
INNER JOIN {{source('LH_MH_Dashboard',"VocationalTrainers")}} AS "vt" ON "vtm"."VTId" = "vt"."VTId" AND "vt"."IsActive" = '1' 		        
INNER JOIN {{source('LH_MH_Dashboard',"VocationalCoordinators")}} AS "vc" ON "vtm"."VCId" = "vc"."VCId" AND "vc"."IsActive" = '1' 
INNER JOIN {{source('LH_MH_Dashboard',"VocationalTrainingProviders")}} AS "vtp" ON "vtm"."VTPId" = "vtp"."VTPId" AND "vtp"."IsActive" = '1' 
INNER JOIN {{source('LH_MH_Dashboard',"AcademicYears")}} AS "ay" ON "vss"."AcademicYearId" = "ay"."AcademicYearId" AND "ay"."IsActive" = '1'          
INNER JOIN {{source('LH_MH_Dashboard',"Sectors")}} AS "se" ON "vss"."SectorId" = "se"."SectorId" AND "se"."IsActive" = '1'        
INNER JOIN {{source('LH_MH_Dashboard',"JobRoles")}} AS "jr" ON "vss"."SectorId" = "jr"."SectorId" AND "vss"."JobRoleId" = "jr"."JobRoleId" AND "jr"."IsActive" = '1'
INNER JOIN {{source('LH_MH_Dashboard',"Schools")}} AS "s" ON "vss"."SchoolId" = "s"."SchoolId" AND "s"."IsActive" = '1'        
INNER JOIN {{source('LH_MH_Dashboard',"AcademicYears")}} AS "say" ON "s"."AcademicYearId" = "say"."AcademicYearId" AND "say"."IsActive" = '1'          
INNER JOIN {{source('LH_MH_Dashboard',"Phases")}} AS "ph" ON "s"."PhaseId" = "ph"."PhaseId" AND "ph"."IsActive" = '1'
INNER JOIN {{source('LH_MH_Dashboard',"Divisions")}} AS "dv" ON "s"."DivisionId" = "dv"."DivisionId" AND "dv"."IsActive" = '1'
INNER JOIN {{source('LH_MH_Dashboard',"Districts")}} AS "ds" ON "s"."DivisionId" = "ds"."DivisionId" AND "s"."DistrictCode" = "ds"."DistrictCode" AND "ds"."IsActive" = '1'		    		 
INNER JOIN {{source('LH_MH_Dashboard',"DataValues")}} AS "dvs" ON "s"."SchoolManagementId" = "dvs"."DataValueId" AND "dvs"."DataTypeId" = 'SchoolManagement'        
INNER JOIN {{source('LH_MH_Dashboard',"SchoolClasses")}} AS "sc" ON "fvc"."ClassTaughtId" = "sc"."ClassId"		                        
LEFT JOIN {{source('LH_MH_Dashboard',"HMSchoolsMap")}} AS "hmm" ON "vss"."AcademicYearId" = "hmm"."AcademicYearId" AND "vss"."SchoolId" = "hmm"."SchoolId" AND "hmm"."DateOfResignation" IS NULL AND "hmm"."IsActive" = '1'		
LEFT JOIN {{source('LH_MH_Dashboard',"HeadMasters")}} AS "hm" ON "hmm"."HMId" = "hm"."HMId" AND "hm"."IsActive" = '1'        
ORDER BY "ay"."YearName", "ph"."PhaseName", "vtp"."VTPName", "vc"."FullName", "vt"."FullName", "fvc"."MonthYear"
