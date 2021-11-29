pt=$1
pt_yesterday=$(date -d"1 day ago $pt" +%Y%m%d)
hive -e "set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite TABLE marketing_modeling.dw_ts_oppor_fail_activation_i partition (pt)
select 
    a.mobile,
    cast(concat(substring('${pt}', 1, 4),'-', substring('${pt}', 5, 2),'-', substring('${pt}', 7, 2)) as timestamp) as action_time,
    '015000000000_tp' as touchpoint_id, -- 战败唤醒
    a.brand,
    '${pt}' as pt
from 
(
    select mobile, brand
    from marketing_modeling.dw_ts_oppor_fail_i
    where 
        pt = '${pt_yesterday}' 
        and touchpoint_id = '014004000000_tp' -- 完全战败
) a
left join 
(
    select mobile, brand, 1 as full_failure_today
    from marketing_modeling.dw_ts_oppor_fail_i
    where 
        pt = '${pt}'
        and touchpoint_id = '014004000000_tp' -- 完全战败
) b
on a.mobile = b.mobile and a.brand = b.brand
where full_failure_today is NULL"