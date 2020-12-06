/*

This is our 'Observation' table. An observation is anytime there has been an interaction with a patient.
This could be a surgery, an image taken, a pathology test administered.
Each row is an observation, with the observation category, fact and datetime recorded.
There the below is formatted as a union between each table and joins to the patient table through SWSUPI 
	and the Encounter table through VISIT_ID.

*/

SELECT   
	[OBS_ID]
	,[SWSUPI]
	,[VISIT_ID]
	,[Observation Category]
	,[Observation Fact]
	,[Observation DtTm]
	--INTO [database_name].[dbo].[aObservation]
FROM (	
--Collect Surginet Observations.
	SELECT  
		'SURGINET ' + [case_num]       COLLATE SQL_Latin1_General_CP1_CI_AS AS [OBS_ID]                 --Collation required on columns 1, 3, and 5 to union all tables.
       ,CAST([AUID] AS INT)                                                 AS [SWSUPI]
	   ,RIGHT('00000000'+[Visit_ID],8) COLLATE SQL_Latin1_General_CP1_CI_AS AS [VISIT_ID]
	   ,'SurgiNet'                                                          AS [Observation Category]
	   ,[Standard_Procedure]           COLLATE SQL_Latin1_General_CP1_CI_AS AS [Observation Fact]
	   ,CAST([Surg_start_dt_tm] AS VARCHAR)                                 AS [Observation DtTm]
	FROM [database_name].[dbo].[aSURGINET]
	WHERE 
		-- Bypassing VISIT_IDs that are not formatted as we would expect from apEvents table.   
		Visit_ID not like '%A%'
		AND 
		Visit_ID not like '%P%'   
		AND 
		Visit_ID not like '%M%'
		AND 
		-- Selecting patients from ED -> Inpatient cohort
		RIGHT('00000000'+[Visit_ID],8) IN (
			SELECT DISTINCT VISIT_ID 
			FROM [database_name].[dbo].[apEvents]
		)
		
	UNION ALL
		
--Collecting Pharmacy observations.
	SELECT  
		('PHARMACY ' + header_id + ' ' + detail_id)    AS [OBS_ID]
       ,SWSUPI
	   ,SWSUPI    COLLATE SQL_Latin1_General_CP1_CI_AS AS [VISIT_ID]
	   ,'Pharmacy'                                     AS [Observation Category]
	   ,[prod_id] COLLATE SQL_Latin1_General_CP1_CI_AS AS [Observation Fact]
	   ,CAST([header_dt] AS VARCHAR)                   AS [Observation DtTm]
	FROM [database_name].[dbo].[aPHARMACY]
	WHERE 
		SWSUPI IN (
			SELECT DISTINCT SWSUPI 
			FROM [database_name].[dbo].[apEvents]
		)
	
	UNION ALL
	
--Collecting Imaging observations.
	SELECT 'IMAGING' + ' ' + [IMAGING].[E_ID]   COLLATE SQL_Latin1_General_CP1_CI_AS AS [OBS_ID]
	       ,[Events].[SWSUPI]                                                        AS [SWSUPI] 
		   ,[IMAGING].[E_ID]                    COLLATE SQL_Latin1_General_CP1_CI_AS AS [VISIT_ID]
		   ,'Imaging'                                                                AS [Observation Category]
		   ,[IMAGING].[PROCEDURE_CODE]          COLLATE SQL_Latin1_General_CP1_CI_AS AS [Observation Fact]
		   ,CAST([IMAGING].[PROCEDURE_START] AS VARCHAR)                             AS [Observation DtTm]
	FROM [database_name].[dbo].[apIMAGING] AS [IMAGING]
	INNER JOIN (SELECT DISTINCT SWSUPI, facility_id, [ed_visit_identifier] FROM [database_name].[dbo].[apEvents]) AS [Events]
				ON CONVERT(varchar,[Events].[facility_id] + '-' + [Events].[ed_visit_identifier]) = [Imaging].[CASE_FOREIGN_ID] 
	
	UNION ALL
	
--Collecting Pathology observations.
	SELECT  
		'PATHOLOGY ' + CAST([ENCNTR_ID] AS VARCHAR)   COLLATE SQL_Latin1_General_CP1_CI_AS AS [OBS_ID]
	   ,[SWSUPI]
	   ,[stay_number]                                 COLLATE SQL_Latin1_General_CP1_CI_AS AS [VISIT_ID]
	   ,'Pathology'                                                                        AS [Observation Category]
	   ,[ORDER_NAME]                                  COLLATE SQL_Latin1_General_CP1_CI_AS AS [Observation Fact]
	   ,CAST([ORDER_PLACED] AS VARCHAR)                                                    AS [Observation DtTm]
	FROM [database_name].[dbo].[aPathologyOrders]   
	WHERE 
		[stay_number] IN (
			SELECT DISTINCT VISIT_ID 
			FROM [database_name].[dbo].[apEvents]
		)

	UNION ALL
	
--Collecting HTRAK observations.
	SELECT 
	   'HTRAK '+ CAST([Procedure ID] AS VARCHAR) COLLATE SQL_Latin1_General_CP1_CI_AS AS [OBS_ID]
	   ,SWSUPI
	   ,SWSUPI                                   COLLATE SQL_Latin1_General_CP1_CI_AS AS [VISIT_ID]
	   ,'HTRAK'                                                                       AS [Observation Category]
	   ,[Description]                            COLLATE SQL_Latin1_General_CP1_CI_AS AS [Observation Fact]
	   ,CAST([Start Date] AS VARCHAR)                                                 AS [Observation DtTm]
	FROM [database_name].[dbo].[aHTRAK]
	WHERE 
		SWSUPI IN (
			SELECT DISTINCT SWSUPI 
			FROM [database_name].[dbo].[apEvents]
		)

) AS [OBSERVATIONS]




