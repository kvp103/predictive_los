USE [database_name]
GO

/****** Object:  StoredProcedure [dbo].[apEvent_Based_Table]    Script Date: 7/11/2019 12:41:22 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



ALTER PROCEDURE [dbo].[apEvent_Based_Table] AS

    DROP TABLE [dbo].[apEvents]

    SELECT 
            -- These first two temp tables remove the 484 patients that had null 
            -- enddatetimes - these could either be patients that are legitimately 
            -- still admitted or clerical errors. Either way, 
            -- these patients need to be removed from the final dataset
            -- as they will blow out the LOS predictions.
        [ed_visit_identifier]
        ,[startdatetime]
        ,[enddatetime]
        ,[days_sequence_number]
        ,[SWSUPI]
        ,ROW_NUMBER() OVER (
            PARTITION BY [ed_visit_identifier]
            ORDER BY [days_sequence_number] DESC
        ) AS [LatestVisit]
        INTO #discharge_temp
    FROM [database_name].[dbo].[apHIE] AS [apHIE]
    WHERE [days_sequence_number] IS NOT NULL

    SELECT 
        [ed_visit_identifier]
        ,[startdatetime]
        ,[enddatetime]
        ,[days_sequence_number]
        ,[SWSUPI]
        INTO #discharge                     
    FROM #discharge_temp AS [Discharged_temp]
    WHERE 
        [Discharged_temp].[LatestVisit] = 1 
        AND 
        [Discharged_temp].[enddatetime] IS NOT NULL

    DROP TABLE #discharge_temp

    SELECT DISTINCT 
            -- select all stays that involve ICU or HDU at some point during the stay,
            -- order by distinct bed type during the stay
        ed_visit_identifier
        ,startdatetime
        ,ROW_NUMBER() OVER (
                PARTITION BY [unit_type], [ed_visit_identifier]
                ORDER BY [startdatetime] ASC
        ) AS [first_time_unit_type]
        ,unit_type
        INTO #icu_hdu_pats
    FROM apHIE
    WHERE 
        ICU_Hours IS NOT NULL
        OR
        HDU_Hours IS NOT NULL

    SELECT 
            -- select all stays that involve ICU or HDU at some point where they entered through ED.
            -- Each visit id lists only the first instance of each bed type during the stay (time ordered).
        #icu_hdu_pats.ed_visit_identifier
        ,#icu_hdu_pats.startdatetime
        ,#icu_hdu_pats.unit_type
        ,ROW_NUMBER() OVER (
            PARTITION BY #icu_hdu_pats.[ed_visit_identifier]
            ORDER BY #icu_hdu_pats.[startdatetime] ASC
        ) AS unit_type_seq
        INTO #ed_icu_hdu_pats
    FROM #icu_hdu_pats
        INNER JOIN ( --This subquery gives us the patients that visited ED first.
            SELECT 
                [ed_visit_identifier]
                ,[unit_type]
                ,ROW_NUMBER() OVER (
                    PARTITION BY [ed_visit_identifier]
                    ORDER BY [startdatetime] ASC
                ) AS [ward_order]
            FROM [database_name].[dbo].[apHIE]
            WHERE [unit_type] =  '17'
        ) AS [ED_first_pats] 
            ON [ED_first_pats].[ed_visit_identifier] = #icu_hdu_pats.[ed_visit_identifier]
            AND [ED_first_pats].[ward_order] = 1 
    WHERE first_time_unit_type = 1

    DROP TABLE #icu_hdu_pats


    /* ##########  Main select statement  ########## */
    SELECT
                -- Since each record in this table should represent one visit or stay,
                -- a roll up of all episodes within one patient stay for LOS fields
                -- are done here.
        (RTRIM(CAST([apHIE].[ed_visit_identifier] AS VARCHAR)) + '-' + 
        CAST(DATEPART(Year, [apHIE].[arrival_time]) AS VARCHAR) + '-' +
        CAST(DATEPART(Month, [apHIE].[arrival_time]) AS VARCHAR) + '-' +
        CAST(DATEPART(Day, [apHIE].[arrival_time]) AS VARCHAR) + '-' +
        CAST(DATEPART(Hour, [apHIE].[arrival_time]) AS VARCHAR)) AS [Encounter_Key]
        ,MIN([apHIE].[startdatetime])                                                AS [start_datetime]
        ,MAX([apHIE].[enddatetime])                                                 AS [end_datetime]
        ,SUM([apHIE].[episode_length_days])                                         AS [episode_length_days]
        ,SUM([apHIE].[episode_length_hours])                                        AS [episode_length_hours]
        ,CONVERT(
            DECIMAL(10,2) 
            ,AVG(CAST(DATEDIFF(MINUTE, arrival_time, actual_departure_time) AS FLOAT)/60)
        )                                                                           AS [ed_length_hours]
        ,CAST(AVG(DATEDIFF(DAY, arrival_time, actual_departure_time)) AS INT)       AS [ed_length_days] 
        ,SUM([apHIE].[episode_length_hours]) - 
        CONVERT(
            DECIMAL(10,2) 
            ,AVG(CAST(DATEDIFF(MINUTE, arrival_time, actual_departure_time) AS FLOAT)/60)
        )                                                                           AS [inpatient_length_hours]
        ,SUM([apHIE].[episode_length_days]) - 
        CAST(AVG(DATEDIFF(DAY, arrival_time, actual_departure_time)) AS INT)        AS [inpatient_length_days]
        ,[apHIE].[ed_v_facility_identifier]                                         AS [facility_id]
        ,[apDFN].[FACILITY_CODE]
        ,[apHIE].[SWSUPI]
        ,[readmitted_within_28_days]              
        ,[apHIE].[ed_visit_identifier]
        ,[latest_entries].[financial_class]
        ,[apHIE].[arrival_time]                                                     AS [ed_arrival_time]
        ,[apHIE].[actual_departure_time]                                            AS [ed_departure_time]
        ,[apDFN].[ARRIVE_DT_TM]                                                      
        ,[apDFN].[ARRIVE_DAY]                                                       AS [ed_arrive_day]  
        ,[apHIE].[indigenous_status]
        ,[apDFN].[ABORIGINALITY]    
        ,[apHIE].[ed_v_birth_date]                                                  AS [patient_dob]
        ,[apDFN].[DOB]    
        ,[apHIE].[mode_of_separation]
        ,[apHIE].[ed_v_sex]
        ,[apDFN].[GENDER]    
        ,[apHIE].[mode_of_arrival]
        ,[apHIE].[need_interpreter_service]
        ,[apHIE].[ed_v_mrn]
        ,[apHIE].[ed_v_patient_postcode]                                            AS [patient_postcode]
        ,[apHIE].[udag_urgency]
        ,[apHIE].[udag_disposition]
        ,[apHIE].[udag_age_group]
        ,[apHIE].[udag_weight]
        ,[apHIE].[ed_v_country_of_usual_residence]
        ,[apHIE].[presenting_problem]
        ,[apHIE].[ed_diagnosis_code]
        ,[latest_entries].[e_drg_an_drg]
        ,[latest_entries].[DX_P]
        ,[latest_entries].[PR_P]
        ,CASE WHEN [ed_to_icu_or_hdu].[does_pat_qualify_icu] = 1 THEN [apHIE].[ICU_Hours] ELSE 0 END 
                                                                                    AS [icu_hours]
        ,CASE WHEN [ed_to_icu_or_hdu].[does_pat_qualify_icu] = 1 THEN 1 ELSE 0 END  AS [icu_status]
            -- need above case statement otherwise will have NULLs instead of 0s. Maybe that's okay actually?
        ,CASE WHEN [ed_to_icu_or_hdu].[does_pat_qualify_hdu] = 1 THEN [apHIE].[HDU_Hours] ELSE 0 END 
                                                                                    AS [hdu_hours]
        ,CASE WHEN [ed_to_icu_or_hdu].[does_pat_qualify_hdu] = 1 THEN 1 ELSE 0 END  AS [hdu_status]
            -- need above case statement otherwise will have NULLs instead of 0s.
        ,[apDFN].[MRN]
        ,[apDFN].[VISIT_ID]
        ,[apDFN].[AMB_INCIDENT_NUMBER]
        ,[apDFN].[ARRIVAL_TO_TRIAGE_TM]
        ,[apDFN].[TO_DOCTOR_TM]
        ,[apDFN].[TO_NURSE_TM]
        ,[apDFN].[TRIAGE_TO_TREAT_TM]
        ,[apDFN].[ADMIT_MED_SERVICE]
        ,[apDFN].[TOTAL_ED_TM]                                                       AS [total_ed_time_min]
        ,[apDFN].[TRIAGE_CATEGORY]
        ,[apDFN].[AMO_NAME]
        ,[apDFN].[PROVIDER_NBR]
        ,[apDFN].[DISCH_DIAG]
        ,[apDFN].[DISCH_DISPOSITION]
        ,[LongestWardStay].[unit_type]                                               AS [longest_unit]
        ,[LongestWardStay].[ward_identifier]                                         AS [longest_ward]
        ,[OrderWardStay].[unit_type]                                                 AS [first_unit]
        ,[OrderWardStay].[ward_identifier]                                           AS [first_ward]
        INTO [dbo].[apEvents]
    FROM [database_name].[dbo].[apHIE] AS [apHIE] 
                    -- [apHIE] table has test case removed
        INNER JOIN [database_name].[dbo].[apDisposition_FirstNet] AS [apDFN] 
                    --apDFN removes the one patient with duplicate records
            ON [apDFN].[FACILITY_CODE] = [apHIE].[ed_v_facility_identifier]
            AND CAST(ROUND([apDFN].[VISIT_ID],0) AS INT) = CAST([apHIE].[ed_visit_identifier] AS INT)
                    -- VISIT_ID is a float in aDFN
        LEFT JOIN ( --This subquery gives us the ward a patient spent the most amount of time (if this is what we choose is the main ward).
            SELECT 
                [ed_visit_identifier]
                ,[unit_type]
                ,[ward_identifier]
                ,SUM([episode_length_hours]) AS [episode_length_hours]
                ,ROW_NUMBER() OVER (
                    PARTITION BY [ed_visit_identifier]
                    ORDER BY SUM([episode_length_hours]) DESC
                ) AS [Ward_Length]
            FROM [database_name].[dbo].[apHIE]
            WHERE [unit_type] <>  '17'
            GROUP BY 
                [unit_type]
                ,[ward_identifier]
                ,[ed_visit_identifier] 
        ) AS [LongestWardStay] 
            ON [LongestWardStay].[ed_visit_identifier] = [apHIE] .[ed_visit_identifier]
            AND [LongestWardStay].[Ward_Length] = 1
        LEFT JOIN ( --This subquery gives us the ward a patient were transfered to from ED (if this is what we choose is the main ward).
            SELECT 
                [ed_visit_identifier]
                ,[unit_type]
                ,[ward_identifier]
                ,[days_sequence_number]
                ,ROW_NUMBER() OVER (
                    PARTITION BY [ed_visit_identifier]
                    ORDER BY [days_sequence_number] DESC
                ) AS [Ward_Order]
            FROM [database_name].[dbo].[apHIE]
            WHERE [unit_type] <>  '17'
        ) AS [OrderWardStay] 
            ON [OrderWardStay].[ed_visit_identifier] = [apHIE].[ed_visit_identifier]
            AND [OrderWardStay].[Ward_Order] = 1
        LEFT JOIN ( -- This subquery removes dupes in Encounter_Key with the exception of a handful of encounters with multiple SWSUPIs entered.
            SELECT  
                [ed_visit_identifier]
                ,[startdatetime]
                ,[enddatetime]
                ,[DX_P]
                ,[PR_P]
                ,[e_drg_an_drg]
                ,[financial_class]
                ,ROW_NUMBER() OVER (
                    PARTITION BY [ed_visit_identifier]
                    ORDER BY [startdatetime] DESC
                ) AS [latest_entry]
            FROM [database_name].[dbo].[apHIE]
        ) AS [latest_entries]
            ON [latest_entries].[ed_visit_identifier] = [apHIE].[ed_visit_identifier]
            AND [latest_entries].[latest_entry] = 1
        LEFT JOIN ( -- This subquery identifies patients that visit ICU or HDU wards straight after ED
            SELECT 
                ed_visit_identifier
                ,CASE WHEN unit_type in ('15','91','92','37') THEN 1 ELSE 0 END as does_pat_qualify_icu
                ,CASE WHEN unit_type in ('34','16') THEN 1 ELSE 0 END as does_pat_qualify_hdu
            FROM #ed_icu_hdu_pats
            WHERE 
                unit_type_seq = 2 
                AND 
                unit_type in ('15','91','92', '37', '34','16')
                    -- These are ICU and HDU bed types. unit_type_seq =2 means straight after ED 
                    -- (since we selected ED for unit_type_seq=1 in #ed_icu_hdu_pats)
        ) AS [ed_to_icu_or_hdu]
            ON [ed_to_icu_or_hdu].[ed_visit_identifier] = [apHIE].[ed_visit_identifier]
    WHERE 
        [apHIE].[days_sequence_number] IS NOT NULL 
        AND 
        [apHIE].[SWSUPI] IS NOT NULL
        AND 
        [apHIE].[SWSUPI] <> ''
                    -- removes patients who haven't been admitted as an inpatient
        AND
        [apHIE].[ed_visit_identifier] NOT IN (  
            SELECT [ed_visit_identifier]
            FROM [database_name].[dbo].[apHIE]
            WHERE [episode_of_care_type] like '%M%'
        )           -- excludes mental health inpatients, their LOS is highly varied
        AND 
        [apHIE].[ed_visit_identifier] IN ( 
            SELECT [ed_visit_identifier] 
            FROM #discharge
        )           -- makes sure we only take patients who have been discharged already
    GROUP BY
        [apHIE].[ed_v_facility_identifier]                                   
        ,[apDFN].[FACILITY_CODE]
        ,[apHIE].[SWSUPI]    
        ,[readmitted_within_28_days]         
        ,[apHIE].[ed_visit_identifier]
        ,[apHIE].[arrival_time] 
        ,[apHIE].[actual_departure_time]
        ,[apDFN].[ARRIVE_DT_TM]                                                
        ,[apDFN].[ARRIVE_DAY]                                                  
        ,[apHIE].[indigenous_status]
        ,[apDFN].[ABORIGINALITY]    
        ,[apHIE].[ed_v_birth_date]                                            
        ,[apDFN].[DOB]    
        ,[apHIE].[mode_of_separation]
        ,[latest_entries].[financial_class]
        ,[apHIE].[ed_v_sex]
        ,[apDFN].[GENDER]    
        ,[apHIE].[mode_of_arrival]
        ,[apHIE].[need_interpreter_service]
        ,[apHIE].[ed_v_mrn]
        ,[apHIE].[ed_v_patient_postcode]                                      
        ,[apHIE].[udag_urgency]
        ,[apHIE].[udag_disposition]
        ,[apHIE].[udag_age_group]
        ,[apHIE].[udag_weight]
        ,[apHIE].[ed_v_country_of_usual_residence]
        ,[apHIE].[presenting_problem]
        ,[latest_entries].[e_drg_an_drg]
        ,[latest_entries].[DX_P]
        ,[latest_entries].[PR_P]
        ,[apHIE].[ed_diagnosis_code]
        ,[ed_to_icu_or_hdu].[does_pat_qualify_icu]
        ,[apHIE].[ICU_Hours]
        ,[ed_to_icu_or_hdu].[does_pat_qualify_hdu]
        ,[apHIE].[HDU_Hours]
        ,[apDFN].[MRN]
        ,[apDFN].[VISIT_ID]
        ,[apDFN].[AMB_INCIDENT_NUMBER]
        ,[apDFN].[ARRIVAL_TO_TRIAGE_TM]
        ,[apDFN].[TO_DOCTOR_TM]
        ,[apDFN].[TO_NURSE_TM]
        ,[apDFN].[TRIAGE_TO_TREAT_TM]
        ,[apDFN].[ADMIT_MED_SERVICE]
        ,[apDFN].[TOTAL_ED_TM]                                                 
        ,[apDFN].[TRIAGE_CATEGORY]
        ,[apDFN].[AMO_NAME]
        ,[apDFN].[PROVIDER_NBR]
        ,[apDFN].[DISCH_DIAG]
        ,[apDFN].[DISCH_DISPOSITION]
        ,[LongestWardStay].[unit_type]
        ,[LongestWardStay].[ward_identifier]
        ,[OrderWardStay].[unit_type]
        ,[OrderWardStay].[ward_identifier]  

    DROP TABLE #discharge
    DROP TABLE #ed_icu_hdu_pats
GO


