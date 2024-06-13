WITH temp_table AS 
(
SELECT  CONCAT("ted"."UDISE","ted"."SectorName",SUBSTR("ted"."AcademicYear",3,2)) AS YUS_Id,
			"ted"."AcademicYear", 
			"ted"."UDISE", 
			"ted"."SchoolName",
			"ted"."DistrictName", 		
			"ted"."SectorName", 
			"ted"."JobRoleName", 
			"ted"."CategoryName",
			"ted"."VTPName", 
			"ted"."VCName", 
			"ted"."VCEmail", 
			"ted"."VTName", 
			"ted"."VTEmail",  
			"ted"."TEReceiveStatus", 
			"ted"."ReceiptDate", 
			"ted"."OATEStatus",
			"ted"."OFTEStatus",
			"ted"."Reason",
			"ted"."IsSelected",
			"ted"."IsSpecify",
			"ted"."RFNReceiveStatus",
			"ted"."IsCommunicated",
			"ted"."IsSetUpWorkShop",
			"ted"."RoomType",
			"ted"."AccommodateTools",
			"ted"."RoomSize",
			"ted"."IsDoorLock",
			"ted"."Flooring",
			"ted"."RoomWindows",
			"ted"."TotalWindowCount",
			"ted"."IsWindowGrills",
			"ted"."IsWindowLocked",
			"ted"."IsRoomActive",
			"ted"."REFInstalled",
			"ted"."WorkingSwitchBoard",
			"ted"."PSSCount",
			"ted"."WLCount",
			"ted"."WFCount",
			"x"."RoomDamaged",
			"ted"."RawMaterialRequired",
			"ted"."ImgToolList",
			"ted"."ImgLab",
			"ted"."Remark",
			"ted"."ToolEquipmentId"
FROM (SELECT DISTINCT 
				"ay"."YearName" AS "AcademicYear", 
				"vtp"."VTPName",		
				"vc"."FullName" AS "VCName", 
				"vc"."EmailId" AS "VCEmail", 
				"vt"."FullName" AS "VTName", 
				"vt"."Email" AS "VTEmail",				
				"ds"."DistrictName", 
				"sc"."CategoryName" AS "CategoryName",
				"s"."UDISE", 
				"s"."SchoolName", 
				"se"."SectorName", 
				"jr"."JobRoleName", 		
				"te"."ReceiptDate",
				"te"."TEReceiveStatus",
				"te"."OATEStatus",
				"te"."OFTEStatus",
				"te"."Reason",
				"te"."IsSelected",
				"te"."IsSpecify",
				"te"."RFNReceiveStatus",
				"te"."IsCommunicated",
				"te"."IsSetUpWorkShop",
				"te"."RoomType",
				"te"."AccommodateTools",
				"te"."RoomSize",
				"te"."IsDoorLock",
				"te"."Flooring",
				"te"."RoomWindows",
				"te"."TotalWindowCount",
				"te"."IsWindowGrills",
				"te"."IsWindowLocked",
				"te"."IsRoomActive",
				"te"."REFInstalled",
				"te"."WorkingSwitchBoard",
				"te"."PSSCount",
				"te"."WLCount",
				"te"."WFCount",
				"te"."RawMaterialRequired",
				"te"."TLFilePath" AS "ImgToolList",
				"te"."LabFilePath" AS "ImgLab",
				"te"."Remarks" AS "Remark",
				"te"."ToolEquipmentId"      
		FROM {{source('LH_MH_Dashboard',"VTSchoolSectors")}} AS "vss"
			INNER JOIN {{source('LH_MH_Dashboard',"VCTrainersMap")}} AS "vtm" ON "vss"."AcademicYearId" = "vtm"."AcademicYearId" 
			AND "vss"."VTId" = "vtm"."VTId" AND "vtm"."DateOfResignation" IS NULL AND "vtm"."IsActive" = '1'
            INNER JOIN {{source('LH_MH_Dashboard',"VocationalTrainers")}} AS "vt" ON "vtm"."VTId" = "vt"."VTId" AND "vt"."IsActive" = '1'			    
        	INNER JOIN {{source('LH_MH_Dashboard',"VocationalCoordinators")}} AS "vc" ON "vtm"."VCId" = "vc"."VCId" AND "vc"."IsActive" = '1'
			INNER JOIN {{source('LH_MH_Dashboard',"VocationalTrainingProviders")}} AS "vtp" ON "vtm"."VTPId" = "vtp"."VTPId" AND "vtp"."IsActive" = '1'
        	INNER JOIN {{source('LH_MH_Dashboard',"Schools")}} AS "s" ON "vss"."SchoolId" = "s"."SchoolId" AND "s"."IsActive" = '1'            	
			INNER JOIN {{source('LH_MH_Dashboard',"SchoolCategories")}} AS "sc" ON "sc"."SchoolCategoryId" = "s"."SchoolCategoryId" AND "sc"."IsActive" = '1'	
			INNER JOIN {{source('LH_MH_Dashboard',"Sectors")}} AS "se" ON "vss"."SectorId" = "se"."SectorId" AND "se"."IsActive" = '1'  
			INNER JOIN {{source('LH_MH_Dashboard',"JobRoles")}} AS "jr" ON "vss"."SectorId" = "jr"."SectorId" AND "vss"."JobRoleId" = "jr"."JobRoleId" AND "jr"."IsActive" = '1'
			INNER JOIN {{source('LH_MH_Dashboard',"AcademicYears")}} AS "ay" ON "vss"."AcademicYearId" = "ay"."AcademicYearId" AND "ay"."IsActive" = '1'                        	
			INNER JOIN {{source('LH_MH_Dashboard',"Divisions")}} AS "dv" ON "s"."DivisionId" = "dv"."DivisionId" AND "dv"."IsActive" = '1'
			INNER JOIN {{source('LH_MH_Dashboard',"Districts")}} AS "ds" ON "s"."DivisionId" = "ds"."DivisionId" AND "s"."DistrictCode" = "ds"."DistrictCode"	
			INNER JOIN {{source('LH_MH_Dashboard',"DataValues")}} AS "dvs" ON "s"."SchoolManagementId" = "dvs"."DataValueId" AND "dvs"."DataTypeId" = 'SchoolManagement'
			LEFT JOIN {{source('LH_MH_Dashboard',"ToolEquipments")}} AS "te" ON "vss"."AcademicYearId" = "te"."AcademicYearId" AND "vss"."VTSchoolSectorId" = "te"."SectorId" 				
			LEFT JOIN {{source('LH_MH_Dashboard',"HMSchoolsMap")}} AS "hmm" ON "vss"."AcademicYearId" = "hmm"."AcademicYearId" 
			AND "vss"."SchoolId" = "hmm"."SchoolId" AND "hmm"."DateOfResignation" IS NULL AND "hmm"."IsActive" = '1'		
			LEFT JOIN {{source('LH_MH_Dashboard',"HeadMasters")}} AS "hm" ON "hmm"."HMId" = "hm"."HMId" AND "hmm"."IsActive" = '1' 		            			
	        WHERE "vss"."IsActive" = '1' AND "vss"."DateOfRemoval" IS NULL AND "ay"."YearName" = '2023-2024'
        ) AS ted
LEFT JOIN (
    		SELECT terd."ToolEquipmentId", STRING_AGG(terd."RoomDamaged", ',') AS "RoomDamaged"
    		FROM {{source('LH_MH_Dashboard',"ToolEquipmentsRoomDamaged")}} AS terd 
    		WHERE terd."IsActive" = '1' 
    		GROUP BY terd."ToolEquipmentId"
    		) AS "x" 
		ON x."ToolEquipmentId" = ted."ToolEquipmentId"
ORDER BY 
    ted."AcademicYear",  
    ted."VTPName", 
    ted."SectorName", 
    ted."VCName", 
    ted."SchoolName", 
    ted."VTName"
)
SELECT * FROM temp_table