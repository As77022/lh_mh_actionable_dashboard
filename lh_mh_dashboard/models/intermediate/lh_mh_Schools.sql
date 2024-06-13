-- School Report from School Information Report
WITH temp_table  AS
(
SELECT DISTINCT
    CASE 
        WHEN "sc"."Name" IS NULL THEN CONCAT("s"."UDISE", "se"."SectorName", SUBSTR("ay"."YearName", 3, 2))
        ELSE CONCAT("s"."UDISE", "se"."SectorName", SUBSTR("ay"."YearName", 3, 2), RIGHT("sc"."Name", 2))
    END AS "YUSC_Id",
    CONCAT("s"."UDISE", "se"."SectorName", SUBSTR("ay"."YearName", 3, 2)) AS "YUS_Id",
    "ay"."YearName" AS "AcademicYear",		
    "ph"."PhaseName",
    "say"."YearName" AS "SchoolAllottedYear",   
    "vtp"."VTPId",
    "vtp"."VTPName",
    "vc"."VCId",
    "vc"."FullName" AS "VCName",
    "vc"."Mobile" AS "VCMobile",
    "vc"."EmailId" AS "VCEmail",  
    "vt"."VTId",
    "vt"."FullName" AS "VTName",
    "vt"."Mobile" AS "VTMobile",
    "vt"."Email" AS "VTEmail",
    "vtm"."DateOfJoining" AS "VTDateOfJoining",
    "hm"."FullName" AS "HMName",
    "hm"."Mobile" AS "HMMobile",
    "hm"."Email" AS "HMEmail",
    "dvs"."Name" AS "SchoolManagement",
    "d"."DivisionName",
    "ds"."DistrictName",
    "s"."BlockName",
    "s"."UDISE",
    "s"."SchoolId",
    "s"."SchoolName",
    "s"."IsImplemented",
    "dvst"."Name" AS "SchoolType",
    "se"."SectorId",
    "se"."SectorName",
    "jr"."JobRoleId",
    "jr"."JobRoleName",
    "sc"."Name" AS "ClassName"
FROM {{source('LH_MH_Dashboard','Schools')}} AS "s"
LEFT JOIN {{source('LH_MH_Dashboard',"SchoolVTPSectors")}} AS "svtps" ON "s"."SchoolId" = "svtps"."SchoolId" AND "svtps"."IsActive" = '1'
LEFT JOIN {{source('LH_MH_Dashboard',"VCSchoolSectors")}} AS "vcss" ON "svtps"."AcademicYearId" = "vcss"."AcademicYearId" AND "svtps"."SchoolVTPSectorId" = "vcss"."SchoolVTPSectorId" AND "vcss"."IsActive" = '1' 
LEFT JOIN {{source('LH_MH_Dashboard',"AcademicYears")}} AS "ay" ON "svtps"."AcademicYearId" = "ay"."AcademicYearId" AND "ay"."IsActive" = '1'        
LEFT JOIN {{source('LH_MH_Dashboard',"AcademicYears")}} AS "say" ON "s"."AcademicYearId" = "say"."AcademicYearId" AND "say"."IsActive" = '1'        
LEFT JOIN {{source('LH_MH_Dashboard',"Divisions")}} AS "d" ON "s"."DivisionId" = "d"."DivisionId"
LEFT JOIN {{source('LH_MH_Dashboard',"Districts")}} AS "ds" ON "s"."DistrictCode" = "ds"."DistrictCode"	
LEFT JOIN {{source('LH_MH_Dashboard',"Phases")}} AS "ph" ON "s"."PhaseId" = "ph"."PhaseId" AND "ph"."IsActive" = '1'
LEFT JOIN {{source('LH_MH_Dashboard',"DataValues")}} AS "dvs" ON "dvs"."DataValueId" = "s"."SchoolManagementId" AND "dvs"."DataTypeId" = 'SchoolManagement' 		
LEFT JOIN {{source('LH_MH_Dashboard',"DataValues")}} AS "dvst" ON "dvst"."DataValueId" = "s"."SchoolTypeId" AND "dvst"."DataTypeId" = 'SchoolType' 		
LEFT JOIN {{source('LH_MH_Dashboard',"VTSchoolSectors")}} AS "vss" ON "svtps"."AcademicYearId" = "vss"."AcademicYearId" AND "svtps"."SchoolId" = "vss"."SchoolId" AND "svtps"."SectorId" = "vss"."SectorId" AND "vss"."IsActive" = '1'		
LEFT JOIN {{source('LH_MH_Dashboard',"Sectors")}} AS "se" ON "svtps"."SectorId" = "se"."SectorId"				
LEFT JOIN {{source('LH_MH_Dashboard',"JobRoles")}} AS "jr" ON "vss"."SectorId" = "jr"."SectorId" AND "vss"."JobRoleId" = "jr"."JobRoleId"
LEFT JOIN {{source('LH_MH_Dashboard',"VTClasses")}} AS "vtc" ON "vss"."AcademicYearId" = "vtc"."AcademicYearId" AND "vss"."SchoolId" = "vtc"."SchoolId" AND "vss"."VTId" = "vtc"."VTId" AND "vtc"."IsActive" = '1'
LEFT JOIN {{source('LH_MH_Dashboard',"SchoolClasses")}} AS "sc" ON "sc"."ClassId" = "vtc"."ClassId"
LEFT JOIN {{source('LH_MH_Dashboard',"VTPAcademicYearsMap")}} AS "vtpm" ON "svtps"."AcademicYearId" = "vtpm"."AcademicYearId" AND "svtps"."VTPId" = "vtpm"."VTPId" AND "vtpm"."IsActive" = '1'
LEFT JOIN {{source('LH_MH_Dashboard',"VocationalTrainingProviders")}} AS "vtp" ON "vtpm"."VTPId" = "vtp"."VTPId" AND "vtp"."IsActive" = '1'		
LEFT JOIN {{source('LH_MH_Dashboard',"VTPCoordinatorsMap")}} AS "vcm" ON "vcss"."AcademicYearId" = "vcm"."AcademicYearId" AND "vcss"."VCId" = "vcm"."VCId" AND "vcm"."IsActive" = '1'
LEFT JOIN {{source('LH_MH_Dashboard',"VocationalCoordinators")}} AS "vc" ON "vcm"."VCId" = "vc"."VCId" AND "vc"."IsActive" = '1'		 
LEFT JOIN {{source('LH_MH_Dashboard',"VCTrainersMap")}} AS "vtm" ON "vss"."AcademicYearId" = "vtm"."AcademicYearId" AND "vss"."VTId" = "vtm"."VTId" AND "vtm"."IsActive" = '1'
LEFT JOIN {{source('LH_MH_Dashboard',"VocationalTrainers")}} AS "vt" ON "vtm"."VTId" = "vt"."VTId" AND "vt"."IsActive" = '1'	
LEFT JOIN {{source('LH_MH_Dashboard',"HMSchoolsMap")}} AS "hmm" ON "svtps"."AcademicYearId" = "hmm"."AcademicYearId" AND "svtps"."SchoolId" = "hmm"."SchoolId" AND "hmm"."IsActive" = '1'		
LEFT JOIN {{source('LH_MH_Dashboard',"HeadMasters")}} AS "hm" ON "hmm"."HMId" = "hm"."HMId"    
WHERE "s"."IsActive" = '1'
GROUP BY "ay"."YearName", "ph"."PhaseName", "SchoolAllottedYear", "vtp"."VTPId", "vtp"."VTPName",
    "vc"."VCId", "vc"."FullName", "vc"."Mobile", "vc"."EmailId", 
    "vt"."VTId", "vt"."FullName", "vt"."Mobile", "vt"."Email", "vtm"."DateOfJoining",
    "hm"."FullName", "hm"."Mobile", "hm"."Email", "dvs"."Name", "d"."DivisionName", "ds"."DistrictName", "s"."BlockName",
    "s"."UDISE", "s"."SchoolId", "s"."SchoolName", "dvst"."Name", "se"."SectorId", "se"."SectorName", "jr"."JobRoleId", "jr"."JobRoleName", "sc"."Name", "s"."IsImplemented"
ORDER BY "ay"."YearName", "ph"."PhaseName", "SchoolAllottedYear", "vtp"."VTPName", "vc"."FullName", "vt"."FullName", "se"."SectorName", "s"."SchoolName"
)
select distinct YUSC_Id from temp_table
