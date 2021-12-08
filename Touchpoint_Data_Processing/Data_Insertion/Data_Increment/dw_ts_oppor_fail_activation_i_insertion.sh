pt=$3
pt_yesterday=$(date -d"1 day ago $pt" +%Y%m%d)
hive -e "set hive.exec.dynamic.partition.mode=nonstrict;
set hive.execution.engine=mr;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join = false;

INSERT overwrite TABLE marketing_modeling.cdm_ts_oppor_fail_activation_i partition (pt,branc)
select 
    a.mobile,
    cast(concat(substring('${pt}', 1, 4),'-', substring('${pt}', 5, 2),'-', substring('${pt}', 7, 2)) as timestamp) as action_time,
    '015000000000_tp' as touchpoint_id, -- 战败唤醒
      '${pt}' as pt,
    a.brand
from 
(
    select mobile, brand
    from marketing_modeling.cdm_ts_oppor_fail_i
    where 
        pt = '${pt_yesterday}' 
        and touchpoint_id = '014004000000_tp' -- 完全战败
) a
left join 
(
    select mobile, brand, 1 as full_failure_today
    from marketing_modeling.cdm_ts_oppor_fail_i
    where 
        pt = '${pt}'
        and touchpoint_id = '014004000000_tp' -- 完全战败
) b
on a.mobile = b.mobile and a.brand = b.brand
where full_failure_today is NULL"