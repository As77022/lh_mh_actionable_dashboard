-- this table containing the data for AY-2023-2024 
WITH CourseMaterialStatusReport AS 
(
    SELECT DISTINCT 
        "ay"."YearName" AS "AcademicYear", 
        "say"."YearName" AS "SchoolAllottedYear",         
        "s"."SchoolName", 
        CONCAT("s"."UDISE", "se"."SectorName", SUBSTR("ay"."YearName", 3, 2), RIGHT("scs"."Name", 2)) AS "YUSC_Id",
        "s"."UDISE", 
        "s"."BlockName", 
        "d"."DivisionName", 
        "ds"."DistrictName", 
        "se"."SectorName", 
        "jr"."JobRoleName", 
        "ph"."PhaseName" AS "phaseName", 
        "vtp"."VTPName",
        "vc"."FullName" AS "VCName", 
        "vc"."Mobile" AS "VCMobile", 
        "vc"."EmailId" AS "VCEmail", 
        "vt"."FullName" AS "VTName", 
        "vt"."Mobile" AS "VTMobile", 
        "vt"."Email" AS "VTEmail",
        "vtm"."DateOfJoining" AS "VTDateOfJoining",
        "hm"."FullName" AS "HMName", 
        "hm"."Mobile" AS "HMMobile", 
        "hm"."Email" AS "HMEmail",
        "dvs"."Name" AS "SchoolManagement",		  
        "scs"."Name" AS "ClassName",        
		"cm"."CMStatus" AS "CourceMaterialAvailability",
		"cm"."ReceiptDate" AS "DateOfReceipt",        
		TO_CHAR("cm"."ReceiptDate", 'FMMonth-YYYY') AS "MonthYear"
    FROM {{source('LH_MH_Dashboard',"VTClasses")}} AS "vtc"
    INNER JOIN {{source('LH_MH_Dashboard',"VTSchoolSectors")}} AS "vss" ON "vtc"."AcademicYearId" = "vss"."AcademicYearId" AND "vtc"."SchoolId" = "vss"."SchoolId" AND "vtc"."VTId" = "vss"."VTId" AND "vss"."IsActive" = '1'
    INNER JOIN {{source('LH_MH_Dashboard',"AcademicYears")}} AS "ay" ON "vtc"."AcademicYearId" = "ay"."AcademicYearId" AND "ay"."IsActive" = '1'	
    INNER JOIN {{source('LH_MH_Dashboard',"Schools")}} AS "s" ON "vtc"."SchoolId" = "s"."SchoolId" AND "vtc"."IsActive" = '1'    
    INNER JOIN {{source('LH_MH_Dashboard',"Sectors")}} AS "se" ON "vss"."SectorId" = "se"."SectorId" AND "se"."IsActive" = '1'
    INNER JOIN {{source('LH_MH_Dashboard',"JobRoles")}} AS "jr" ON "vss"."JobRoleId" = "jr"."JobRoleId" AND "jr"."IsActive" = '1'	 
    INNER JOIN {{source('LH_MH_Dashboard',"AcademicYears")}} AS "say" ON "s"."AcademicYearId" = "say"."AcademicYearId" AND "say"."IsActive" = '1'	
    INNER JOIN {{source('LH_MH_Dashboard',"SchoolClasses")}} AS "scs" ON "vtc"."ClassId" = "scs"."ClassId"
    INNER JOIN {{source('LH_MH_Dashboard',"Divisions")}} AS "d" ON "s"."DivisionId" = "d"."DivisionId"
    INNER JOIN {{source('LH_MH_Dashboard',"Districts")}} AS "ds" ON "s"."DistrictCode" = "ds"."DistrictCode"
    INNER JOIN {{source('LH_MH_Dashboard',"Phases")}} AS "ph" ON "s"."PhaseId" = "ph"."PhaseId" AND "ph"."IsActive" = '1'
    INNER JOIN {{source('LH_MH_Dashboard',"DataValues")}} AS "dvs" ON "s"."SchoolManagementId" = "dvs"."DataValueId" AND "dvs"."DataTypeId" = 'SchoolManagement' 	
    INNER JOIN {{source('LH_MH_Dashboard',"VCTrainersMap")}} AS "vtm" ON "vtc"."AcademicYearId" = "vtm"."AcademicYearId" AND "vtc"."VTId" = "vtm"."VTId" AND "vtm"."IsActive" = '1'
    INNER JOIN {{source('LH_MH_Dashboard',"VTPCoordinatorsMap")}} AS "vcm" ON "vtm"."AcademicYearId" = "vcm"."AcademicYearId" AND "vtm"."VCId" = "vcm"."VCId" AND "vcm"."IsActive" = '1'
    INNER JOIN {{source('LH_MH_Dashboard',"VTPAcademicYearsMap")}} AS "vtpm" ON "vcm"."AcademicYearId" = "vtpm"."AcademicYearId" AND "vcm"."VTPId" = "vtpm"."VTPId" AND "vtpm"."IsActive" = '1'
    LEFT JOIN {{source('LH_MH_Dashboard',"VocationalTrainers")}} AS "vt" ON "vtm"."VTId" = "vt"."VTId" 
    LEFT JOIN {{source('LH_MH_Dashboard',"VocationalCoordinators")}} AS "vc" ON "vcm"."VCId" = "vc"."VCId"
    LEFT JOIN {{source('LH_MH_Dashboard',"VocationalTrainingProviders")}} AS "vtp" ON "vtpm"."VTPId" = "vtp"."VTPId" 
    LEFT JOIN {{source('LH_MH_Dashboard',"CourseMaterials")}} AS "cm" ON "vtc"."AcademicYearId" = "cm"."AcademicYearId" AND "vtc"."VTClassId" = "cm"."ClassId" 
    LEFT JOIN {{source('LH_MH_Dashboard',"HMSchoolsMap")}} AS "hmm" ON "vtc"."AcademicYearId" = "hmm"."AcademicYearId" AND "vtc"."SchoolId" = "hmm"."SchoolId" AND "hmm"."IsActive" = '1'
    LEFT JOIN {{source('LH_MH_Dashboard',"HeadMasters")}} AS "hm" ON "hmm"."HMId" = "hm"."HMId" AND "hm"."IsActive" = '1'
    WHERE "vtc"."IsActive" = '1' 
)
SELECT * FROM CourseMaterialStatusReport AS "cms"
WHERE  "cms"."AcademicYear" = '2023-2024' AND
       "cms"."AcademicYear" IS NOT NULL AND
       "DivisionName" IS NOT NULL AND 
       "SectorName" IS NOT NULL AND 
       "JobRoleName" IS NOT NULL AND 
       "VTPName" IS NOT NULL AND 
       "ClassName" IS NOT NULL AND 
       "SchoolManagement" IS NOT NULL AND 
       "HMName" IS NOT NULL AND 
       "SchoolName" IS NOT NULL 
ORDER BY "AcademicYear", "SchoolAllottedYear", "phaseName", "VTPName", "VCName", "SchoolName", "VTName"