#!/bin/bash
pt2=$3
pre_day=$4
pt1=$(date -d "${pt2} -$pre_day day" '+%Y%m%d')
cd $(dirname $(readlink -f $0))
queue_name=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  config.ini`
hive --hivevar pt1=$pt1 --hivevar pt2=$pt2 --hivevar queue_name=${queue_name} -e "
set tez.queue.name=${queue_name};
set hive.exec.dynamic.partition.mode=nonstrict;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.exec.max.dynamic.partitions=2048;
set hive.exec.max.dynamic.partitions.pernode=1000;

set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join = false;

insert overwrite table marketing_modeling.cdm_ts_app_activity_i partition (pt,brand)
select
mobile,
action_time,
touchpoint_id,
cast(pt as string) pt,
cast(brand as string) brand
from
(
    select
        enter_user_phone as mobile, 
        create_date as action_time,
        case
            when brand_code = 2 then '008002006001_tp'  -- 社交裂变类参与
            when brand_code = 1 then '008002002001_rw'
        else NULL end as touchpoint_id,
        regexp_replace(to_date(create_date), '-', '') as pt,
        case
            when brand_code = 2 then 'MG'
            when brand_code = 1 then 'RW'
        else NULL end as brand
    from 
    (
        select 
            enter_user_phone, create_date, activity_code
        from dtwarehouse.ods_bbs_fission_network
        where 
            pt = '${pt2}'
            and regexp_replace(to_date(create_date), '-', '') >= ${pt1}
            and regexp_replace(to_date(create_date), '-', '') <= ${pt2}
            and enter_user_phone != ''
    ) a 
    left join
    (select activity_code, brand_code from dtwarehouse.ods_bbs_fission_activity where pt = '${pt2}') b
    on a.activity_code = b.activity_code

    union all

    select 
        mobile_phone as mobile,
        create_date as action_time,
        '008002006002_tp' as touchpoint_id, -- 发起答题活动
        regexp_replace(to_date(create_date), '-', '') as pt,
        'MG' as brand
    from dtwarehouse.ods_db_supply_tb_help_ask
    where 
        pt = '${pt2}'
        and regexp_replace(to_date(create_date), '-', '') >= ${pt1}
        and regexp_replace(to_date(create_date), '-', '') <= ${pt2}
        and brand_code = 2
        
    union all 

    select 
        mobile_phone as mobile,
        create_date as action_time,
        '008002006003_tp' as touchpoint_id, -- 参与答题活动
         regexp_replace(to_date(create_date), '-', '') as pt,
        'MG' as brand
    from dtwarehouse.ods_db_supply_tb_help_reply
    where 
        pt = '${pt2}'
        and regexp_replace(to_date(create_date), '-', '') >= ${pt1}
        and regexp_replace(to_date(create_date), '-', '') <= ${pt2}
        and brand_code = 2
        
    union all 

    select 
        cellphone as mobile,
        create_time as action_time,
        '008002006004_tp' as touchpoint_id, -- 社区活动类参与
        regexp_replace(to_date(create_time), '-', '') as pt,
        'MG' as brand
    from 
    (
        select activity_id, user_id, create_time
        from dtwarehouse.ods_bbscomm_tt_saic_activity_interested 
        where 
            pt = '${pt2}'
            and regexp_replace(to_date(create_time), '-', '') >= ${pt1}
            and regexp_replace(to_date(create_time), '-', '') <= ${pt2}
    ) a 
    left join 
    (
        select id, brand_type 
        from dtwarehouse.ods_bbscomm_tt_saic_activity 
        where 
            pt = '${pt2}' and brand_type = '2'
    ) b 
    on a.activity_id = b.id
    join 
    (
        select 
            cellphone, uid
        from 
        (
            select 
                cellphone, uid, 
                Row_Number() OVER (partition by uid ORDER BY regist_date) rank_num 
            from dtwarehouse.ods_ccm_member
            where 
                pt = '${pt2}' -- 生产环境使用
                and cellphone IS NOT NULL
                and uid IS NOT NULL
        ) b0
        where rank_num = 1 
    ) c
    on a.user_id = c.uid
) t1
where
    mobile regexp '^[1][3-9][0-9]{9}$'
	and action_time IS NOT NULL
	and touchpoint_id IS NOT NULL
    and brand IS NOT NULL
"