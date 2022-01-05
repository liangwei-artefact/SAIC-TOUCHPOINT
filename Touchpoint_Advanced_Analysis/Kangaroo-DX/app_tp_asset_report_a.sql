#MG
insert into app_tp_asset_report_a ( touchpoint_level ,fir_contact_month ,fir_contact_tp_id ,fir_contact_series ,mac_code ,rfs_code ,area ,tp_pv ,tp_uv ,instore_vol ,trial_vol ,consume_vol ,cust_vol ,exit_pv ,undeal_vol ,pt ,brand ) values ( ? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,#{pt} ,'MG' )
ON DUPLICATE KEY UPDATE
touchpoint_level =values(touchpoint_level )
,fir_contact_month =values(fir_contact_month )
,fir_contact_tp_id =values(fir_contact_tp_id )
,fir_contact_series =values(fir_contact_series )
,mac_code =values(mac_code )
,rfs_code =values(rfs_code )
,area =values(area )
,tp_pv =values(tp_pv )
,tp_uv =values(tp_uv )
,instore_vol =values(instore_vol )
,trial_vol =values(trial_vol )
,consume_vol =values(consume_vol )
,cust_vol =values(cust_vol )
,exit_pv =values(exit_pv )
,undeal_vol =values(undeal_vol )
,pt =values(pt )
,brand =values(brand )
#{pt}=${yyyyMM};#{BUSINESS_DATE}=${yyyy-MM-dd HH:mm:ss};#{__SUB_JOB_DATE__}=${yyyyMMdd}
#2,17,18,19,20,21,22,23,24
#RW
insert into app_tp_asset_report_a ( touchpoint_level ,fir_contact_month ,fir_contact_tp_id ,fir_contact_series ,mac_code ,rfs_code ,area ,tp_pv ,tp_uv ,instore_vol ,trial_vol ,consume_vol ,cust_vol ,exit_pv ,undeal_vol ,pt ,brand ) values ( ? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,#{pt} ,'RW' )
ON DUPLICATE KEY UPDATE
touchpoint_level =values(touchpoint_level )
,fir_contact_month =values(fir_contact_month )
,fir_contact_tp_id =values(fir_contact_tp_id )
,fir_contact_series =values(fir_contact_series )
,mac_code =values(mac_code )
,rfs_code =values(rfs_code )
,area =values(area )
,tp_pv =values(tp_pv )
,tp_uv =values(tp_uv )
,instore_vol =values(instore_vol )
,trial_vol =values(trial_vol )
,consume_vol =values(consume_vol )
,cust_vol =values(cust_vol )
,exit_pv =values(exit_pv )
,undeal_vol =values(undeal_vol )
,pt =values(pt )
,brand =values(brand )
#{pt}=${yyyyMM};#{BUSINESS_DATE}=${yyyy-MM-dd HH:mm:ss};#{__SUB_JOB_DATE__}=${yyyyMMdd}
#2,17,18,19,20,21,22,23,24
