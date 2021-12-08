pt1=$1
pt2=$2
hive --hivevar pt1=$pt1 --hivevar pt2=$pt2 -e "set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamici.partition=true;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
SET hive.exec.max.dynamic.partitions=2048;
SET hive.exec.max.dynamic.partitions.pernode=1000;
set hive.execution.engine=mr;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join = false;

INSERT overwrite TABLE marketing_modeling.cdm_ts_complete_oppor_fail_i partition (pt)
select * from
(
select 
     aa.mobile,
     cast(concat(substring(aa.pt, 1, 4),'-', substring(aa.pt, 5, 2),'-', substring(aa.pt, 7, 2)) as timestamp) as action_time,
     '014004000000_tp' as touchpoint_id, -- 完全战败
     case 
         when aa.brand_id = 121 then 'MG' 
         when aa.brand_id = 101 then 'RW'
         else NULL
     end as brand,
     aa.pt
from 
(
    select 
        mobile, 
        brand_id,
        oppor_cnt,
        pt
    from marketing_modeling.tmp_ods_dlm_t_oppor_agg_a
    where 
        pt <= '${pt2}' and pt >= '${pt1}'
) aa
left join
(
    select mobile, brand_id, fail_oppor_cnt, pt
    from marketing_modeling.tmp_ods_dlm_t_oppor_fail_agg_a
    where 
        pt <= '${pt2}' and pt >= '${pt1}'
) bb
on aa.mobile = bb.mobile and aa.brand_id = bb.brand_id and aa.pt = bb.pt
where fail_oppor_cnt is NULL or fail_oppor_cnt >= oppor_cnt
) result
where
    mobile regexp '^[1][3-9][0-9]{9}$'
	and action_time is not NULL
	and touchpoint_id is not NULL
"


