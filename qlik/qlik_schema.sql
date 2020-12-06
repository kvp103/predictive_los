LIB CONNECT TO [server_name];

DRG_Mapping:
MAPPING LOAD
    LEFT("DRG  Description", 4) AS [e_drg_an_drg],
    RIGHT("DRG  Description", LEN("DRG  Description")-5) AS [DRG_desc]
FROM [lib://AttachedFiles/DRG_Mapping.xlsx]
(ooxml, embedded labels, table is Sheet1);

Qualify *;
Unqualify [SURGI_LINK_KEY], [CASE_FOREIGN_ID], [PATHOLOGY_KEY], [PHARM_KEY], [HTRAK_KEY], [DFN_KEY];


[apEvents]:
LOAD
	[start_datetime],
	[end_datetime],
    (Date(monthstart([end_datetime]), 'MMM-YYYY'))              AS [Discharge_MonthYear],
    NUM(episode_length_days) 									AS [episode_length_days],
    NUM(episode_length_hours) 									AS [episode_length_hours],
    ROUND(NUM(episode_length_hours),5)							AS [bucket_length_hours],
    ROUND(NUM(episode_length_days),5) 							AS [bucket_length_days],
    [ed_length_hours],
    [ed_length_days],
    [inpatient_length_hours],
    [inpatient_length_days],
	[facility_id] AS [facility_id],
	[FACILITY_CODE] AS [FACILITY_CODE],
	[SWSUPI] AS [SWSUPI],
	[ed_visit_identifier],
	[ed_arrival_time],
    [ed_departure_time],
	Timestamp(Timestamp#([ARRIVE_DT_TM], 'DD/MM/YYYY hh:mm') )  AS [ARRIVE_DT_TM],
	[ed_arrive_day],
	[indigenous_status],
	[ABORIGINALITY],
    IF([ABORIGINALITY]='1', 'Aboriginal but not Torres Strait Islander origin',
    	IF([ABORIGINALITY]='2', 'Torres Strait Islander but not Aboriginal origin',
        IF([ABORIGINALITY]='3', 'Both Aboriginal & Torres Strait Islander origin',
        IF([ABORIGINALITY]='4', 'Neither Aboriginal nor Torres Strait Islander origin',
        IF([ABORIGINALITY]='8', 'Declined to Respond',
        IF([ABORIGINALITY]='9', 'Unknown', 'Other'))))))        AS [Aboriginal_desc],
	Timestamp([patient_dob] ) 									AS [patient_dob],
	Timestamp([DOB] ) 											AS [DOB],
	[mode_of_separation],
	[ed_v_sex],
	[GENDER] AS [GENDER],
	[mode_of_arrival],
    [mode_of_arrival_desc],
	[need_interpreter_service],
	[ed_v_mrn],
	[patient_postcode],
    IF([patient_postcode] > 200 AND [patient_postcode] <299, 'ACT',
    	IF([patient_postcode] > 799  AND [patient_postcode] <1000, 'NT',
    	IF([patient_postcode] > 999  AND [patient_postcode] <3000, 'NSW',
    	IF([patient_postcode] > 2999 AND [patient_postcode] <4000, 'VIC',
    	IF([patient_postcode] > 3999 AND [patient_postcode] <5000, 'QLD',
    	IF([patient_postcode] > 4999 AND [patient_postcode] <6000, 'SA',
    	IF([patient_postcode] > 5999 AND [patient_postcode] <7000, 'WA',
    	IF([patient_postcode] > 6999 AND [patient_postcode] <8000, 'TAS',
    	IF([patient_postcode] > 7999 AND [patient_postcode] <9000, 'VIC',
    	IF([patient_postcode] > 8999 AND [patient_postcode] <10000, 'QLD', 'Other')))))))))) as [patient_state],
	[udag_urgency],
	[udag_disposition],
	[udag_age_group]											AS [AGE_MAPPING_KEY],
    IF([udag_age_group] = '1',0-14,
    	IF([udag_age_group] = '2',15-34,
        IF([udag_age_group] = '3','35-64',
        IF([udag_age_group] = '4', '65+', 'Other'))))           AS [age_render],
	[udag_weight],
	[ed_v_country_of_usual_residence],
	[presenting_problem],
	[ed_diagnosis_code],
    [e_drg_an_drg],
    ApplyMap('DRG_Mapping', [e_drg_an_drg],'Unknown')           AS [DRG_desc],
	[DISCH_DIAG],
	[DISCH_DISPOSITION],
    [DX_P],
    [PR_P],
    [readmitted_within_28_days],
	[ICU_Status],
	[ICU_Hours],
	[HDU_Status],
	[HDU_Hours],
	[MRN] 														AS [MRN],
	[VISIT_ID] 													AS [VISIT_ID],
	[AMB_INCIDENT_NUMBER] AS [AMB_INCIDENT_NUMBER],
    NUM([ARRIVAL_TO_TRIAGE_TM])									AS [ARRIVAL_TO_TRIAGE_TM],
	NUM([TO_DOCTOR_TM])											AS [TO_DOCTOR_TM],
	NUM([TO_NURSE_TM])											AS [TO_NURSE_TM],
	NUM([TRIAGE_TO_TREAT_TM])									AS [TRIAGE_TO_TREAT_TM],
    [ADMIT_MED_SERVICE],
	[total_ed_time_min],
	[TRIAGE_CATEGORY] AS [TRIAGE_CATEGORY],
	[AMO_NAME] AS [AMO_NAME],
	[PROVIDER_NBR] AS [PROVIDER_NBR],
    NUM(SWSUPI,'0000000000') & NUM(VISIT_ID)                   AS SURGI_LINK_KEY,
    facility_id & '-' & NUM(VISIT_ID)			                AS CASE_FOREIGN_ID,
    SWSUPI & NUM(VISIT_ID) 						                AS PATHOLOGY_KEY,
	SWSUPI & NUM(VISIT_ID)								        AS PHARM_KEY,
    NUM(VISIT_ID) & SWSUPI                                      AS DFN_KEY,
    SWSUPI									                    AS HTRAK_KEY;
SQL SELECT "start_datetime",
	"end_datetime",
	"episode_length_days",
	"episode_length_hours",
    "ed_length_hours",
    "ed_length_days",
    "inpatient_length_hours",
    "inpatient_length_days",
	"facility_id",
	"FACILITY_CODE",
	"SWSUPI",
	"ed_visit_identifier",
	"ed_arrival_time",
    "ed_departure_time",
	"ARRIVE_DT_TM",
	"ed_arrive_day",
	"indigenous_status",
	"ABORIGINALITY",
	"patient_dob",
	"DOB",
	"mode_of_separation",
	"ed_v_sex",
	"GENDER",
	"mode_of_arrival",
    "mode_of_arrival_desc",
	"need_interpreter_service",
	"ed_v_mrn",
	"patient_postcode",
	"udag_urgency",
	"udag_disposition",
	"udag_age_group",
	"udag_weight",
	"ed_v_country_of_usual_residence",
	"presenting_problem",
	"ed_diagnosis_code",
    "e_drg_an_drg",
	"DISCH_DIAG",
	"DISCH_DISPOSITION",
    "DX_P",
    "PR_P",
    "readmitted_within_28_days",
	"ICU_Status",
	"ICU_Hours",
	"HDU_Status",
	"HDU_Hours",
	"MRN",
	"VISIT_ID",
	"AMB_INCIDENT_NUMBER",
    "ARRIVAL_TO_TRIAGE_TM",
	"TO_DOCTOR_TM",
	"TO_NURSE_TM",
	"TRIAGE_TO_TREAT_TM",
    "ADMIT_MED_SERVICE",
	"total_ed_time_min",
	"TRIAGE_CATEGORY",
	"AMO_NAME",
	"PROVIDER_NBR"
FROM "database_name"."dbo"."apEvents";



#surginet

[aSUR]:
LOAD
	[AUID],
	[MRN] AS [aSURGINET.MRN],
	[Facility],
	[Enc_facility],
	[Standard_Procedure],
	[Other_Procedures],
	[Visit_ID],
	[case_num],
	[Specialty],
	[procedural_Consultant],
	[Age],
	Date(Date#([dob], 'DD/MM/YYYY') ) AS [aSURGINET.dob],
	[sex] AS [aSURGINET.sex],
	[fin_class],
	[Case_Type],
	[Emerg_priority],
	[Surgical_Area],
	[Room],
	[codes],
	[code_type],
	[code_source],
	[Actual_Procedure],
	[planned_Procedure],
	[Anaes_List],
	[Anaes_Type],
	[Wound_Class],
	Timestamp(Timestamp#([pat_in_rm_dt_tm], 'DD/MM/YYYY hh:mm') ) AS [pat_in_rm_dt_tm],
	Timestamp(Timestamp#([pat_out_rm_dt_tm], 'DD/MM/YYYY hh:mm') ) AS [pat_out_rm_dt_tm],
	Timestamp(Timestamp#([Anaes_start_dt_tm], 'DD/MM/YYYY hh:mm') ) AS [Anaes_start_dt_tm],
	Timestamp(Timestamp#([Anaes_stop_dt_tm], 'DD/MM/YYYY hh:mm') ) AS [Anaes_stop_dt_tm],
	Num([ANAES_DURATION_MIN]) AS [ANAES_DURATION_MIN],
	Timestamp(Timestamp#([Surg_start_dt_tm], 'DD/MM/YYYY hh:mm') ) AS [Surg_start_dt_tm],
	Timestamp(Timestamp#([Surg_stop_dt_tm], 'DD/MM/YYYY hh:mm') ) AS [Surg_stop_dt_tm],
	Timestamp(Timestamp#([pacu_in_rm_dt_tm], 'DD/MM/YYYY hh:mm') ) AS [pacu_in_rm_dt_tm],
	Timestamp(Timestamp#([pacu_out_rm_dt_tm], 'DD/MM/YYYY hh:mm') ) AS [pacu_out_rm_dt_tm],
	[pacu_report_name],
	[Actual_duration],
	Timestamp(Timestamp#([finalised_dt_tm], 'DD/MM/YYYY hh:mm') ) AS [finalised_dt_tm],
	[unplanned_return],
	[Ward],
	[FACILITY_CODE] AS [aSURGINET.FACILITY_CODE],
    AUID & Visit_ID AS SURGI_LINK_KEY
WHERE Visit_ID <> '11084987';
SQL SELECT "AUID",
	"MRN",
	"Facility",
	"Enc_facility",
	"Standard_Procedure",
	"Other_Procedures",
	"Visit_ID",
	"case_num",
	"Specialty",
	"procedural_Consultant",
	"Age",
	"dob",
	"sex",
	"fin_class",
	"Case_Type",
	"Emerg_priority",
	"Surgical_Area",
	"Room",
	"codes",
	"code_type",
	"code_source",
	"Actual_Procedure",
	"planned_Procedure",
	"Anaes_List",
	"Anaes_Type",
	"Wound_Class",
	"pat_in_rm_dt_tm",
	"pat_out_rm_dt_tm",
	"Anaes_start_dt_tm",
	"Anaes_stop_dt_tm",
	"ANAES_DURATION_MIN",
	"Surg_start_dt_tm",
	"Surg_stop_dt_tm",
	"pacu_in_rm_dt_tm",
	"pacu_out_rm_dt_tm",
	"pacu_report_name",
	"Actual_duration",
	"finalised_dt_tm",
	"unplanned_return",
	"Ward",
	"FACILITY_CODE"
FROM "database_name"."dbo"."aSURGINET"
;

#imaging

[aImaging]:
LOAD
	[PROCEDURE_KEY],
	[INSTITUTE_KEY],
	[REQUEST_EVENT_DATE],
	[PROCEDURE_START],
	[PROCEDURE_END],
	[PATIENT_CONDITION_CODE],
	[ADMISSION_TYPE],
	[PROCEDURE_CODE],
	[PRIORITY_CODE],
    IF(WILDMATCH(PRIORITY_CODE, 'Semiurg*'), 'SEMIURGENT',
    	IF(WILDMATCH(PRIORITY_CODE, 'IMMED*'), 'IMMEDIATE', PRIORITY_CODE)) AS [PRIORITY],
	[REF_SOURCE_NAME],
	[MED_LOCATION_NAME],
	[PRIVATE_NAME],
	[PRIVATE_CODE],
	[SERVICE_NAME],
    IF(WildMatch([SERVICE_NAME], '*XRay*', '*X-Ray*', 'XR*'), 'X-Ray',
      IF(WildMatch([SERVICE_NAME], 'CT*'), 'CT',
      IF(WildMatch([SERVICE_NAME], 'US*', '*Ultrasound*'), 'US',
      IF(WildMatch([SERVICE_NAME], 'MR*'), 'MR',
      IF(WildMatch([SERVICE_NAME], 'Nucl*'), 'Nuclear',
      IF(WildMatch([SERVICE_NAME], 'Interv*'), 'Intervention',
      IF(WildMatch([SERVICE_NAME], 'PET*'), 'PET',
      IF(WildMatch([SERVICE_NAME], 'fluoro*'), 'Fluoroscopy',
      IF(WildMatch([SERVICE_NAME], 'Theatre*'), 'Theatre',
      IF(WildMatch([SERVICE_NAME], 'ECG*'), 'ECG',
      IF(WildMatch([SERVICE_NAME], 'IN*'), 'IN', 'Other')))))))))))     AS [Service Category],
	[SERVICE_CODE],
	[ACC_ITEM],
	[CASE_FOREIGN_ID],
	[FACILITY_ID],
	[E_ID],
	Timestamp([PAT_BIRTH_DATE] ) AS [PAT_BIRTH_DATE],
	[PAT_SEX],
	[PROCEDURE_DELETED_YN]
Where NOT WildMatch([PRIORITY_CODE],'LH*');    ;
SQL SELECT "PROCEDURE_KEY",
	"INSTITUTE_KEY",
	"REQUEST_EVENT_DATE",
	"PROCEDURE_START",
	"PROCEDURE_END",
	"PATIENT_CONDITION_CODE",
	"ADMISSION_TYPE",
	"PROCEDURE_CODE",
	"PRIORITY_CODE",
	"REF_SOURCE_NAME",
	"MED_LOCATION_NAME",
	"PRIVATE_NAME",
	"PRIVATE_CODE",
	"SERVICE_NAME",
	"SERVICE_CODE",
	"ACC_ITEM",
	"CASE_FOREIGN_ID",
	"FACILITY_ID",
	"E_ID",
	"PAT_BIRTH_DATE",
	"PAT_SEX",
	"PROCEDURE_DELETED_YN"
FROM "database_name"."dbo"."apImaging";

#pathology

[aPath]:
LOAD
	[SWSUPI] AS [aPathologyOrders.SWSUPI],
	[ENCNTR_ID] AS [aPathologyOrders.ENCNTR_ID],
	[ORDER_NAME],
	[PRIORITY],
	[ORDER_PLACED],
	[ORDER_COMPLETED],
	[ORDER_COLLECTED],
	[ORDERS_PLACED_ON_DISCHARGE],
	[ORDERS_COLLECTED_ON_DISCHARGE],
	[ORDERING_PROVIDER_PERSON_ID],
	[ORDERING_PROVIDER_NAME_FIRST],
	[ORDERING_PROVIDER_NAME_LAST],
	[VERIFIED_DT_TM],
	[facility_identifier],
	[stay_number],
	[mrn] AS [aPathologyOrders.mrn],
	[u_encntr_id],
    SWSUPI & stay_number AS PATHOLOGY_KEY;
SQL SELECT "SWSUPI",
	"ENCNTR_ID",
	"ORDER_NAME",
	"PRIORITY",
	"ORDER_PLACED",
	"ORDER_COMPLETED",
	"ORDER_COLLECTED",
	"ORDERS_PLACED_ON_DISCHARGE",
	"ORDERS_COLLECTED_ON_DISCHARGE",
	"ORDERING_PROVIDER_PERSON_ID",
	"ORDERING_PROVIDER_NAME_FIRST",
	"ORDERING_PROVIDER_NAME_LAST",
	"VERIFIED_DT_TM",
	"facility_identifier",
	"stay_number",
	"mrn",
	"u_encntr_id"
FROM "database_name"."dbo"."aPathologyOrders";

#pharmacy

[aPharmacy]:
LOAD
	[facility_id] AS [aPharmacy.facility_id],
	[mrn] AS [aPharmacy.mrn],
	[SWSUPI] AS [aPharmacy.SWSUPI],
	[class],
	[site_xid],
	[site],
	[sex] AS [aPharmacy.sex],
	Date(Date#([dob], 'DD-MM-YYYY') ) AS [aPharmacy.dob],
	[pat_eps_xid],
	[header_id],
	[rev_dspn_id],
	[detail_id],
	[dspn_grp_desc],
	[type],
	[permit],
	Date(Date#([header_dt], 'DD-MM-YYYY') ) AS [header_dt],
	[last_updated_tm],
	[prod_id],
	[generic_desc],
	[brand_desc],
	[strength],
	[form],
	[inv_uom],
	[nbr_ingreds],
	[own_supp_ind],
	[inv_qty],
	[cost],
	[status],
	[style],
	[ward],
	[ward_fullname],
	[oracle_code],
	[dr],
	[dr_nbr],
	[dr_nbr_type],
	[team],
    SWSUPI & [pat_eps_xid] AS PHARM_KEY;
SQL SELECT "facility_id",
	"mrn",
	"SWSUPI",
	"class",
	"site_xid",
	"site",
	"sex",
	"dob",
	"pat_eps_xid",
	"header_id",
	"rev_dspn_id",
	"detail_id",
	"dspn_grp_desc",
	"type",
	"permit",
	"header_dt",
	"last_updated_tm",
	"prod_id",
	"generic_desc",
	"brand_desc",
	"strength",
	"form",
	"inv_uom",
	"nbr_ingreds",
	"own_supp_ind",
	"inv_qty",
	"cost",
	"status",
	"style",
	"ward",
	"ward_fullname",
	"oracle_code",
	"dr",
	"dr_nbr",
	"dr_nbr_type",
	"team"
FROM "database_name"."dbo"."aPharmacy";

#htrak

[aHTRAK]:
LOAD
	[Procedure ID],
	[Patient Ref ],
	[Procedure Name],
	[Financial Class],
	[Cost Centre Code Specialty],
	[Hospital],
	[Department],
	[Facility],
    [FACILITY_CODE] & ' ' & [Department] AS [Facility-Department],
	[Item Codes],
	Date(Date#([Start Date], 'DD-MM-YYYY') ) AS [Start Date],
	[Start Time],
	[Duration (h)],
	[Facility Cost],
	[Hand Held User],
	[Theatre Case Number],
	[FACILITY_CODE],
	[MRN],
	[SWSUPI],
	Date(Date#([Procedure Date], 'DD-MM-YYYY') ) AS [Procedure Date],
	[Supplier],
	[SPC],
	[Description],
	[Qty],
	[Stock Type],
	[Scanned UPN],
	[Oracle Catalogue No ],
	[Cost Centre],
	[OU Category],
	[Prostheses Rebate Code],
	[Consumption Type],
	[Lot Number],
	Date(Date#([Expiry Date], 'DD-MM-YYYY') ) AS [Expiry Date],
	[Total Cost],
	Timestamp(Timestamp#([End Time], 'DD-MM-YYYY hh:mm:ss') ) AS [End Time],
	[Duration],
	[Anaesthetic Key],
	[Anaesthetic Description],
	[Anaesthetic Abrev ],
	Timestamp(Timestamp#([Scan Date Time], 'DD-MM-YYYY hh:mm:ss') ) AS [Scan Date Time],
    SWSUPI				 AS HTRAK_KEY;
SQL SELECT "Procedure ID",
	"Patient Ref ",
	"Procedure Name",
	"Financial Class",
	"Cost Centre Code Specialty",
	"Hospital",
	"Department",
	"Facility",
	"Item Codes",
	"Start Date",
	"Start Time",
	"Duration (h)",
	"Facility Cost",
	"Hand Held User",
	"Theatre Case Number",
	"FACILITY_CODE",
	"MRN",
	"SWSUPI",
	"Procedure Date",
	"Supplier",
	"SPC",
	"Description",
	"Qty",
	"Stock Type",
	"Scanned UPN",
	"Oracle Catalogue No ",
	"Cost Centre",
	"OU Category",
	"Prostheses Rebate Code",
	"Consumption Type",
	"Lot Number",
	"Expiry Date",
	"Total Cost",
	"End Time",
	"Duration",
	"Anaesthetic Key",
	"Anaesthetic Description",
	"Anaesthetic Abrev ",
	"Scan Date Time"
FROM "database_name"."dbo"."aHTRAK";