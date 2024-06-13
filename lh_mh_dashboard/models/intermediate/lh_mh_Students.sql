-- students (table name from dashbard(MH))
WITH temp_table AS
(
SELECT DISTINCT "ay"."YearName" AS "AcademicYear", 
            CONCAT(("s"."UDISE"),("sec"."SectorName"),SUBSTR(("ay"."YearName"),3,2),RIGHT(("cls"."Name"),2)) AS YUSC_Id,
		    "sc"."StudentId" AS "LHStudentId", "sc"."StudentUniqueId", 
			"sc"."FirstName", "sc"."MiddleName", "sc"."LastName", 
			"sc"."FullName" AS "StudentName", "dvg"."Name" AS "StudentGender", 
			"sc"."DateOfEnrollment", "srd"."WhatsAppNo", 
            "sc"."Mobile" AS "PrimaryContact", "srd"."Mobile" AS "AlternativeContact", 
			"cls"."Name" AS "ClassName", "se"."Name" AS "SectionName", "dstm"."Name" AS "StreamName", 
			"srd"."StudentRollNumber" AS "RoleNo", "srd"."FatherName",
            "srd"."MotherName", "srd"."GuardianName", "srd"."DateOfBirth", 
			"dvs"."Name" AS "SocialCategory", "dvr"."Name" AS "Religion", "srd"."IsStudentVE9And10", 
			"srd"."IsSameStudentTrade", "srd"."CWSNStatus",
            "srd"."AssessmentConducted" AS "ReadyForAssesment", "s"."UDISE", 
			"s"."SchoolName", "st"."StateName", "d"."DivisionName", "ds"."DistrictName", 
			"s"."BlockName", "dvsm"."Name" AS "SchoolManagement", "say"."YearName" AS "SchoolAllottedYear",
            "ph"."PhaseName", "hm"."FullName" AS "HMName", "hm"."Mobile" AS "HMMobile",
			"hm"."Email" AS "HMEmail", "sec"."SectorName", "jr"."JobRoleName", 
            "vtp"."VTPName", "vc"."FullName" AS "VCName", "vc"."Mobile" AS "VCMobile", 
			"vc"."EmailId" AS "VCEmail", "vt"."FullName" AS "VTName", "vt"."Mobile" AS "VTMobile",
            "vt"."Email" AS "VTEmail", DATE("vtm"."DateOfJoining") AS "VTDateOfJoining",
			"sc"."CreatedOn", "sc"."UpdatedOn"
		FROM {{source('LH_MH_Dashboard',"StudentClasses")}} AS "sc"			
        INNER JOIN {{source('LH_MH_Dashboard',"StudentClassMapping")}} AS "scm" ON "sc"."SchoolId" = "scm"."SchoolId" 
                    AND "sc"."StudentId" = "scm"."StudentId" AND "scm"."IsActive" = '1'
		INNER JOIN {{source('LH_MH_Dashboard',"VTClasses")}} AS "vtc" ON "scm"."AcademicYearId" = "vtc"."AcademicYearId" 
                    AND "scm"."SchoolId" = "vtc"."SchoolId" AND "scm"."ClassId" = "vtc"."ClassId" AND "scm"."VTId" = "vtc"."VTId" AND "vtc"."IsActive" = '1'	
		INNER JOIN {{source('LH_MH_Dashboard',"VTSchoolSectors")}} AS "vss" ON "vtc"."AcademicYearId" = "vss"."AcademicYearId" 
                    AND "vtc"."SchoolId" = "vss"."SchoolId" AND "vtc"."VTId" = "vss"."VTId" AND "vss"."DateOfRemoval" IS NULL AND "vss"."IsActive" = '1'   				
		INNER JOIN {{source('LH_MH_Dashboard',"Schools")}} AS "s" ON "sc"."SchoolId" = "s"."SchoolId" AND "s"."IsActive" = '1' 
		INNER JOIN {{source('LH_MH_Dashboard',"VCTrainersMap")}} AS "vtm" ON "vss"."AcademicYearId" = "vtm"."AcademicYearId" 
                    AND "vss"."VTId" = "vtm"."VTId" AND "vtm"."DateOfResignation" IS NULL AND "vtm"."IsActive" = '1'
        INNER JOIN {{source('LH_MH_Dashboard',"VocationalTrainers")}} AS "vt" ON "vtm"."VTId" = "vt"."VTId" AND "vt"."IsActive" = '1'
        --INNER JOIN {{source('LH_MH_Dashboard',"VTPCoordinatorsMap")}} AS "vcm" ON "vtm"."AcademicYearId" = "vcm"."AcademicYearId" AND "vtm"."VTPId" = "vcm"."VTPId" AND "vtm"."VCId" = "vcm"."VCId" AND "vcm"."DateOfResignation" IS NULL AND "vcm"."IsActive" = '1'
		INNER JOIN {{source('LH_MH_Dashboard',"VocationalCoordinators")}} AS "vc" ON "vtm"."VCId" = "vc"."VCId" AND "vc"."IsActive" = '1' 
		--INNER JOIN {{source('LH_MH_Dashboard',"VTPAcademicYearsMap")}} AS "vtpm" ON "vcm"."AcademicYearId" = "vtpm"."AcademicYearId" AND "vcm"."VTPId" = "vtpm"."VTPId" AND "vtpm"."DateOfResignation" IS NULL AND "vtpm"."IsActive" = '1'
        INNER JOIN {{source('LH_MH_Dashboard',"VocationalTrainingProviders")}} AS "vtp" ON "vtm"."VTPId" = "vtp"."VTPId" AND "vtp"."IsActive" = '1'
		INNER JOIN {{source('LH_MH_Dashboard',"Sectors")}} AS "sec" ON "vss"."SectorId" = "sec"."SectorId" AND "sec"."IsActive" = '1'  
		INNER JOIN {{source('LH_MH_Dashboard',"JobRoles")}} AS "jr" ON "vss"."SectorId" = "jr"."SectorId" AND "vss"."JobRoleId" = "jr"."JobRoleId" AND "jr"."IsActive" = '1'	
		INNER JOIN {{source('LH_MH_Dashboard',"SchoolClasses")}} AS "cls" ON "scm"."ClassId" = "cls"."ClassId" AND "cls"."IsActive" = '1'
		INNER JOIN {{source('LH_MH_Dashboard',"AcademicYears")}} AS "ay" ON "vss"."AcademicYearId" = "ay"."AcademicYearId" AND "ay"."IsActive" = '1'   
		INNER JOIN {{source('LH_MH_Dashboard',"AcademicYears")}} AS "say" ON "s"."AcademicYearId" = "say"."AcademicYearId" AND "say"."IsActive" = '1'   
        INNER JOIN {{source('LH_MH_Dashboard',"Sections")}} AS "se" ON "scm"."SectionId" = "se"."SectionId"
		INNER JOIN {{source('LH_MH_Dashboard',"DataValues")}} AS "dvg" ON "sc"."Gender" = "dvg"."DataValueId" AND "dvg"."DataTypeId" = 'StudentGender'		
		INNER JOIN {{source('LH_MH_Dashboard',"Phases")}} AS "ph" ON "s"."PhaseId" = "ph"."PhaseId" AND "ph"."IsActive" = '1'
		INNER JOIN {{source('LH_MH_Dashboard',"States")}} AS "st" ON "s"."StateCode" = "st"."StateCode"
        INNER JOIN {{source('LH_MH_Dashboard',"Divisions")}} AS "d" ON "s"."DivisionId" = "d"."DivisionId"
		INNER JOIN {{source('LH_MH_Dashboard',"Districts")}} AS "ds" ON "s"."DistrictCode" = "ds"."DistrictCode"
		INNER JOIN {{source('LH_MH_Dashboard',"DataValues")}} AS "dvsm" ON "s"."SchoolManagementId" = "dvsm"."DataValueId" AND "dvsm"."DataTypeId" = 'SchoolManagement'		
        LEFT JOIN {{source('LH_MH_Dashboard',"StudentClassDetails")}} AS "srd" ON "sc"."StudentId" = "srd"."StudentId" 		
		LEFT JOIN {{source('LH_MH_Dashboard',"DataValues")}} AS "dvs" ON "srd"."SocialCategory" = "dvs"."DataValueId" AND "dvs"."DataTypeId" = 'SocialCategory'
		LEFT JOIN {{source('LH_MH_Dashboard',"DataValues")}} AS "dvr" ON "srd"."Religion" = "dvr"."DataValueId" AND "dvr"."DataTypeId" = 'Religion'						
        LEFT JOIN {{source('LH_MH_Dashboard',"DataValues")}} AS "dstm" ON "srd"."StreamId" = "dstm"."DataValueId" AND "dstm"."DataTypeId" = 'Streams'	
		LEFT JOIN {{source('LH_MH_Dashboard',"HMSchoolsMap")}} AS "hsm" ON "vss"."AcademicYearId" = "hsm"."AcademicYearId" 
                    AND "vss"."SchoolId" = "hsm"."SchoolId" AND "hsm"."DateOfResignation" IS NULL AND "hsm"."IsActive" = '1'
		LEFT JOIN {{source('LH_MH_Dashboard',"HeadMasters")}} AS "hm" ON "hsm"."HMId" = "hm"."HMId"
)
SELECT * FROM temp_table