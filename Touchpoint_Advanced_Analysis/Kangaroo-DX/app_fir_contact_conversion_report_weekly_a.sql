insert into  app_fir_contact_conversion_report_weekly_a ( mac_code ,rfs_code ,area ,is_sec_net ,activity_name ,fir_contact_tp_id ,fir_contact_series ,fir_contact_week ,cust_vol ,instore_vol ,trial_vol ,consume_vol ,deliver_vol ,brand ,pt) values ( ? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,#{pt} );
ON DUPLICATE KEY UPDATE
mac_code =values(mac_code )
,rfs_code =values(rfs_code )
,area =values(area )
,is_sec_net =values(is_sec_net )
,activity_name =values(activity_name )
,fir_contact_tp_id =values(fir_contact_tp_id )
,fir_contact_series =values(fir_contact_series )
,fir_contact_week =values(fir_contact_week )
,cust_vol =values(cust_vol )
,instore_vol =values(instore_vol )
,trial_vol =values(trial_vol )
,consume_vol =values(consume_vol )
,deliver_vol =values(deliver_vol )
,brand =values(brand )
,pt=values(pt)