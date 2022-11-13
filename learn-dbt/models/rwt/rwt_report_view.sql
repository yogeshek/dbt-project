
{{config(materialized='view')}}

SELECT `RWT_MSID`,`RWT_SNO`,`RWT_MEASURE`,`RWT_FIELD`,
(select count(*) from RWT.RWT_REPORT.AUDIT_LOG
 where CONVERT_TIMEZONE('US/Pacific', 'GMT',
   `Transaction_Date_And_Time`::timestamp_ntz) between {{ var('start_date') }} and {{ var('end_date') }}
    and `entity_name` ='EXPORT'
and `Transaction_Type`  ='GENERATE - CCD') AS COUNTS
from rwt_report where `RWT_MSID`='A' AND `RWT_SNO`=1
union
SELECT `RWT_MSID`,`RWT_SNO`,`RWT_MEASURE`,`RWT_FIELD`,
 (select count(*) from RWT.RWT_REPORT.AUDIT_LOG
 where CONVERT_TIMEZONE('US/Pacific', 'GMT',
 `Transaction_Date_And_Time`::timestamp_ntz) between {{ var('start_date') }} and {{ var('end_date') }}
  and `entity_name` ='BULK EXPORT'
and `Transaction_Type`  ='GENERATE') AS COUNTS
 from rwt_report where `RWT_MSID`='A' AND `RWT_SNO`=2

 union

 SELECT `RWT_MSID`,`RWT_SNO`,`RWT_MEASURE`,`RWT_FIELD`,
  (select count(*) from RWT.RWT_REPORT.ACTIVITY_LOG
 where CONVERT_TIMEZONE('US/Pacific', 'GMT',
  `Activity_Date_And_Time`::timestamp_ntz) between {{ var('start_date') }} and {{ var('end_date') }}
  and `activity_type` ='Send Message' and `From_Address` in
 (select `Physician_EMail` from Physician_Library p, user u where p.`physician_library_id`=u.`physician_library_id`
 and u.`physician_library_id` <>0 and p.`Physician_EMail` <>'' and u.`Status`='A')) as COUNTS
  from rwt_report where `RWT_MSID`='A' AND `RWT_SNO`=3

 union

 SELECT `RWT_MSID`,`RWT_SNO`,`RWT_MEASURE`,`RWT_FIELD`,
  (select count(*) from RWT.RWT_REPORT.ACTIVITY_LOG
 where CONVERT_TIMEZONE('US/Pacific', 'GMT',
    `Activity_Date_And_Time`::timestamp_ntz) between {{ var('start_date') }} and {{ var('end_date') }}
 and `activity_type` ='Send Message' and `Sent_To` in
  (select `Physician_EMail` from Physician_Library p, user u where p.`physician_library_id`=u.`physician_library_id`
 and u.`physician_library_id` <>0 and p.`Physician_EMail` <>'' and u.`Status`='A')) as COUNTS
  from rwt_report where `RWT_MSID`='A' AND `RWT_SNO`=4

  union

SELECT `RWT_MSID`,`RWT_SNO`,`RWT_MEASURE`,`RWT_FIELD`,
 (select count(*) from RWT.RWT_REPORT.ACTIVITY_LOG
where CONVERT_TIMEZONE('US/Pacific', 'GMT', `Activity_Date_And_Time`::timestamp_ntz) between {{ var('start_date') }} and {{ var('end_date') }}
 and `activity_type` ='CCD INCORPORATED' ) as COUNTS
 from rwt_report where `RWT_MSID`='A' AND `RWT_SNO`=5

union

SELECT `RWT_MSID`,`RWT_SNO`,`RWT_MEASURE`,`RWT_FIELD`,
 (select count(*) from RWT.RWT_REPORT.ACTIVITY_LOG
where CONVERT_TIMEZONE('US/Pacific', 'GMT', `Activity_Date_And_Time`::timestamp_ntz) between {{ var('start_date') }} and {{ var('end_date') }}
and `activity_type` ='CCD IMPORT' ) as COUNTS
 from rwt_report where `RWT_MSID`='A' AND `RWT_SNO`=6

union

SELECT `RWT_MSID`,`RWT_SNO`,`RWT_MEASURE`,`RWT_FIELD`,
 (select count(*) from RWT.RWT_REPORT.AUDIT_LOG
 where CONVERT_TIMEZONE('US/Pacific', 'GMT', `Transaction_Date_And_Time`::timestamp_ntz) between {{ var('start_date') }} and {{ var('end_date') }}
and `entity_name` ='CLINICAL RECONCILIATION' and `Transaction_Type`  ='SAVE') AS COUNTS
 from rwt_report where `RWT_MSID`='A' AND `RWT_SNO`=7

 ORDER BY `RWT_SNO`
