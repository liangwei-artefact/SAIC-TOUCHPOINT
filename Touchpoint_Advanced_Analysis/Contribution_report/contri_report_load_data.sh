#/*********************************************************************
#*模块: /Touchpoint_Advanced_Analysis/Contribution_Report
#*程序: contri_report_load_data.sh
#*功能: 从hive中读取计算触点转化所需的数据并存储成csv
#*开发人: Boyan
#*开发日: 2021-09-05
#*修改记录:
#*
#*********************************************************************/

pt=$1
pt_month=$(date -d "${pt}" +%Y%m)
cur_month_start=$(date -d "${pt_month}01" +%Y%m%d)
cur_month_end=$(date -d "${cur_month_start} +1 month -1 day" +%Y%m%d)
af_month_end=$(date -d "${cur_month_end} +6 month -1 day" +%Y%m%d)

queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  ../../config/config.ini`

echo "pt:" $pt
echo "pt_month:" $pt_month
echo "cur_month_start:" $cur_month_start
echo "cur_month_end:" $cur_month_end
echo "af_month_end:" $af_month_end


hive -hivevar queuename=queuename -e "
set tez.queue.name=${queuename};
select 
	mobile 
from dtwarehouse.cdm_dim_dealer_employee_info 
where mobile regexp '^[1][3-9][0-9]{9}$' 
group by mobile
" > mobile_to_remove.csv


hive -hivevar queuename=queuename --hivevar cur_month_start=$cur_month_start --hivevar cur_month_end=$cur_month_end -e "
set tez.queue.name=${queuename};
select
    mobile,
    fir_contact_month,
    fir_contact_date,
    fir_contact_series,
    mac_code,
    rfs_code,
	area,
    fir_contact_tp_id
from marketing_modeling.app_touchpoints_profile_monthly
where pt = ${$pt_month} and brand = 'MG'
" > MG_filtered_profile_df.csv


hive -hivevar queuename=queuename --hivevar cur_month_start=$cur_month_start --hivevar cur_month_end=$cur_month_end -e "
set tez.queue.name=${queuename};
select
    mobile,
    fir_contact_month,
    fir_contact_date,
    fir_contact_series,
    mac_code,
    rfs_code,
	area,
    fir_contact_tp_id
from marketing_modeling.app_touchpoints_profile_monthly
where pt = ${$pt_month} and brand = 'RW'
" > RW_filtered_profile_df.csv


hive -hivevar queuename=queuename --hivevar cur_month_start=$cur_month_start --hivevar af_month_end=$af_month_end -e "
set tez.queue.name=${queuename};
select
    mobile,
    action_time,
    touchpoint_id as tp_id
from marketing_modeling.cdm_mg_tp_ts_all_i
where pt >= ${cur_month_start} and pt <= ${af_month_end} and brand = 'MG'
" > MG_all_touchpoint_df.csv


hive -hivevar queuename=queuename --hivevar cur_month_start=$cur_month_start --hivevar af_month_end=$af_month_end -e "
set tez.queue.name=${queuename};
select
    mobile,
    action_time,
    touchpoint_id as tp_id
from marketing_modeling.cdm_rw_tp_ts_all_i
where pt >= ${cur_month_start} and pt <= ${af_month_end} and brand = 'RW'
" > RW_all_touchpoint_df.csv


hive -hivevar queuename=queuename -e "
set tez.queue.name=${queuename};
select
    touchpoint_id as tp_id,
    level_1_tp_id,
    level_2_tp_id,
    level_3_tp_id,
    level_4_tp_id
from marketing_modeling.cdm_touchpoints_id_system
" > touchpoint_df.csv
