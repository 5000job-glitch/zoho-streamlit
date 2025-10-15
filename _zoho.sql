--ZOHO_LEAD_TO_CALLS
with agents as (select
    agentskilltargetid,
    departmentname,
    fullname
from RETAIL_PROD.STAGE.V_S_BOE_TJ_EMPLOYEE_CURRENT
where agentskilltargetid is NOT NULL
and departmentname = 'TALENT ACQUISITION'),

ob_calls as (
select tcd_datetime as call_datetime,
tcd_digitsdialed,
REGEXP_REPLACE(digitsdialed, '[^0-9]', '') as cleaned_phone_number,
RIGHT(cleaned_phone_number, 10) as lean_phone,
tcd_agentskilltargetid,
agents.fullname,
talktime + holdtime as handle_time
from DATA_LAKE.PCCE.CALL_DETAIL_CURRENT tcd
inner join agents using(agentskilltargetid)
where
datetime > '2023-01-01'
and peripheralcalltype in (9, 8, 12, 15, 13)
),
zoho_leads as (
    select created_time as lead_created,
    REGEXP_REPLACE(phone, '[^0-9]', '') AS cleaned_phone_number,
    RIGHT(cleaned_phone_number, 10) as lean_phone,
    phone,
    vendor,
    campaign_name
    FROM RETAIL_PROD.STAGE.V_DLBV_API_ZOHO_CANDIDATES_CURRENT
)

select
    zoho.lead_created,
    zoho.lean_phone,
    zoho.vendor,
    zoho.campaign_name,
    calls.call_datetime,
    calls.agentskilltargetid,
    calls.fullname,
    calls.handle_time,
    DATEDIFF(second, calls.call_datetime, zoho.lead_created) as time_to_call
    -- EXTRACT(EPOCH FROM calls.call_datetime::TIMESTAMP_TZ - lead_created)
FROM zoho_leads as zoho
Left join ob_calls as calls USING(lean_phone);