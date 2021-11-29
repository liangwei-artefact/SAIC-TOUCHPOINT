pt=$3
hive --hivevar pt=$pt -e "
set hive.exec.dynamic.partition.mode=nonstrict;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.exec.max.dynamic.partitions=2048;
set hive.exec.max.dynamic.partitions.pernode=1000;

insert overwrite table marketing_modeling.dw_ts_app_activity_i partition (pt,brand)
select * from
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
            pt = '${pt}' 
            and regexp_replace(to_date(create_date), '-', '') >= ${pt}
            and enter_user_phone != ''
    ) a 
    left join
    (select activity_code, brand_code from dtwarehouse.ods_bbs_fission_activity where pt = '${pt}') b
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
        pt = '${pt}'
        and regexp_replace(to_date(create_date), '-', '') >= ${pt}
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
        pt = '${pt}'
        and regexp_replace(to_date(create_date), '-', '') >= ${pt}
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
            pt = '${pt}'
            and regexp_replace(to_date(create_time), '-', '') >= ${pt}
    ) a 
    left join 
    (
        select id, brand_type 
        from dtwarehouse.ods_bbscomm_tt_saic_activity 
        where 
            pt = '${pt}'
            and brand_type = '2'
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
                pt = '${pt}'
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