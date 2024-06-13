
WITH cte AS (
    SELECT 
    sr."AcademicYearId", 
    sr."VTId", 
    sr."SchoolId", 
    cls."Name" AS "ClassName",
    COUNT(DISTINCT CASE WHEN sr."DateOfDropout" IS NULL AND sr."IsActive" = '1' THEN sr."StudentId" ELSE NULL END) AS "TotalEnrollmentStudents",
    SUM(CASE WHEN sr."DateOfDropout" IS NULL AND sr."IsActive" = '1' AND sr."Gender" = '207' THEN 1 ELSE 0 END) AS "EnrolledBoys", 
    SUM(CASE WHEN sr."DateOfDropout" IS NULL AND sr."IsActive" = '1' AND sr."Gender" = '208' THEN 1 ELSE 0 END) AS "EnrolledGirls", 
    SUM(CASE WHEN sr."IsActive" = '0' AND sr."DateOfDropout" IS NOT NULL THEN 1 ELSE 0 END) AS "Dropout"
FROM  (
        SELECT DISTINCT 
            scm."AcademicYearId", 
            sc."SchoolId", 
            sc."ClassId", 
            sc."StudentId", 
            sc."Gender", 
            scm."VTId", 
            sc."DateOfDropout", 
            sc."DeletedBy", 
            sc."IsActive"
        FROM {{source('LH_MH_Dashboard',"StudentClassMapping")}} AS scm 
        INNER JOIN {{source('LH_MH_Dashboard',"StudentClasses")}} AS sc ON scm."SchoolId" = sc."SchoolId" AND scm."SectionId" = sc."SectionId" AND scm."StudentId" = sc."StudentId" AND "DeletedBy" IS NULL AND sc."IsActive" = '1'
        INNER JOIN {{source('LH_MH_Dashboard',"VTClasses")}} AS vtc ON scm."AcademicYearId" = vtc."AcademicYearId" AND scm."SchoolId" = vtc."SchoolId" AND scm."ClassId" = vtc."ClassId" AND scm."VTId" = vtc."VTId" AND vtc."IsActive" = '1'    
        INNER JOIN {{source('LH_MH_Dashboard',"VTClassSections")}} AS vtcs ON vtc."VTClassId" = vtcs."VTClassId"
        INNER JOIN {{source('LH_MH_Dashboard',"VTSchoolSectors")}} AS vss ON vtc."AcademicYearId" = vss."AcademicYearId" AND vtc."SchoolId" = vss."SchoolId" AND vtc."VTId" = vss."VTId" AND vss."IsActive" = '1'              
        INNER JOIN {{source('LH_MH_Dashboard',"Schools")}} AS s ON sc."SchoolId" = s."SchoolId" AND s."IsActive" = '1' 
        INNER JOIN {{source('LH_MH_Dashboard',"AcademicYears")}} AS ar ON scm."AcademicYearId" = ar."AcademicYearId" AND ar."IsActive" = '1'   
        INNER JOIN {{source('LH_MH_Dashboard',"SchoolClasses")}} AS cls ON sc."ClassId" = cls."ClassId"
        INNER JOIN {{source('LH_MH_Dashboard',"Sections")}} AS se ON sc."SectionId" = se."SectionId"
        INNER JOIN {{source('LH_MH_Dashboard',"DataValues")}} AS dvg ON sc."Gender" = dvg."DataValueId" AND dvg."DataTypeId" = 'StudentGender'            
        LEFT JOIN {{source('LH_MH_Dashboard',"VCTrainersMap")}} AS vtm ON scm."AcademicYearId" = vtm."AcademicYearId" AND scm."VTId" = vtm."VTId" AND vtm."DateOfResignation" IS NULL AND vtm."IsActive" = '1'
        LEFT JOIN {{source('LH_MH_Dashboard',"VTPCoordinatorsMap")}} AS vcm ON vtm."AcademicYearId" = vcm."AcademicYearId" AND vtm."VTPId" = vcm."VTPId" AND vtm."VCId" = vcm."VCId" AND vcm."DateOfResignation" IS NULL AND vcm."IsActive" = '1'
    ) AS sr
    INNER JOIN {{source('LH_MH_Dashboard',"SchoolClasses")}} AS cls ON sr."ClassId" = cls."ClassId"
    GROUP BY sr."AcademicYearId", sr."SchoolId", sr."VTId", sr."ClassId",cls."Name"
)

SELECT DISTINCT 
    concat(s."UDISE",se."SectorName", substr(ay."YearName",3,2),right(sc."ClassName",2))  AS "usyc_id",
    ay."YearName" AS "AcademicYear",
    /* say."YearName" AS "SchoolAllottedYear",
    ph."PhaseName",
    vtp."VTPName", 
    vt."FullName" AS "VTName",
    vt."Mobile" AS "VTMobile",
    vt."Email" AS "VTEmail",
    DATE(vtm."DateOfJoining") AS "VTDateOfJoining",
    vc."FullName" AS "VCName",
    vc."Mobile" AS "VCMobile",
    vc."EmailId" AS "VCEmail",
    hm."FullName" AS "HMName",
    hm."Mobile" AS "HMMobile",
    hm."Email" AS "HMEmail",
    dvs."Name" AS "SchoolManagement",
    d."DivisionName",
    ds."DistrictName",
    s."BlockName", */
    s."UDISE",
    s."SchoolName", 
    se."SectorName",
    jr."JobRoleName",            
    sc."ClassName", 
    sc."TotalEnrollmentStudents", 
    sc."EnrolledBoys", 
    sc."EnrolledGirls",
    sc."Dropout"
FROM cte sc
INNER JOIN {{source('LH_MH_Dashboard',"AcademicYears")}} AS ay ON sc."AcademicYearId" = ay."AcademicYearId" AND ay."IsActive" = '1'
INNER JOIN {{source('LH_MH_Dashboard',"Schools")}} AS s ON sc."SchoolId" = s."SchoolId" AND s."IsActive" = '1'        
INNER JOIN {{source('LH_MH_Dashboard',"AcademicYears")}} AS say ON s."AcademicYearId" = say."AcademicYearId" AND say."IsActive" = '1'
INNER JOIN {{source('LH_MH_Dashboard',"Phases")}} AS ph ON s."PhaseId" = ph."PhaseId" AND ph."IsActive" = '1'
INNER JOIN {{source('LH_MH_Dashboard',"Divisions")}} AS d ON s."DivisionId" = d."DivisionId"
INNER JOIN {{source('LH_MH_Dashboard',"Districts")}} AS ds ON s."DistrictCode" = ds."DistrictCode"
INNER JOIN {{source('LH_MH_Dashboard',"DataValues")}} AS dvs ON s."SchoolManagementId" = dvs."DataValueId" AND dvs."DataTypeId" = 'SchoolManagement'     
INNER JOIN {{source('LH_MH_Dashboard',"VTSchoolSectors")}} AS vss ON sc."AcademicYearId" = vss."AcademicYearId" AND sc."SchoolId" = vss."SchoolId" AND sc."VTId" = vss."VTId" AND vss."IsActive" = '1'         
LEFT JOIN {{source('LH_MH_Dashboard',"Sectors")}} AS se ON vss."SectorId" = se."SectorId"
LEFT JOIN {{source('LH_MH_Dashboard',"JobRoles")}} AS jr ON vss."SectorId" = jr."SectorId" AND vss."JobRoleId" = jr."JobRoleId"        
LEFT JOIN {{source('LH_MH_Dashboard',"VCTrainersMap")}} AS vtm ON vss."AcademicYearId" = vtm."AcademicYearId" AND vss."VTId" = vtm."VTId" AND vtm."DateOfResignation" IS NULL AND vtm."IsActive" = '1'
LEFT JOIN {{source('LH_MH_Dashboard',"VocationalTrainers")}} AS vt ON vtm."VTId" = vt."VTId" AND vt."IsActive" = '1' 
LEFT JOIN {{source('LH_MH_Dashboard',"VTPCoordinatorsMap")}} AS vcm ON vtm."AcademicYearId" = vcm."AcademicYearId" AND vtm."VCId" = vcm."VCId" AND vcm."DateOfResignation" IS NULL AND vcm."IsActive" = '1'
LEFT JOIN {{source('LH_MH_Dashboard',"VocationalCoordinators")}} AS vc ON vcm."VCId" = vc."VCId" AND vc."IsActive" = '1'  
LEFT JOIN {{source('LH_MH_Dashboard',"VTPAcademicYearsMap")}} AS vtpm ON vcm."AcademicYearId" = vtpm."AcademicYearId" AND vcm."VTPId" = vtpm."VTPId" AND vtpm."DateOfResignation" IS NULL AND vtpm."IsActive" = '1'
LEFT JOIN {{source('LH_MH_Dashboard',"VocationalTrainingProviders")}} AS vtp ON vtpm."VTPId" = vtp."VTPId" AND vtp."IsActive" = '1' 
LEFT JOIN {{source('LH_MH_Dashboard',"HMSchoolsMap")}} AS hmm ON vss."AcademicYearId" = hmm."AcademicYearId" AND vss."SchoolId" = hmm."SchoolId" AND hmm."IsActive" = '1'        
LEFT JOIN {{source('LH_MH_Dashboard',"HeadMasters")}} AS hm ON hmm."HMId" = hm."HMId" AND hm."IsActive" = '1'         
ORDER BY ay."YearName",s."SchoolName", se."SectorName"
limit 10