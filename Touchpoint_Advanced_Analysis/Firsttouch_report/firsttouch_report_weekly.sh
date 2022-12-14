#!/bin/bash
#/*********************************************************************
#*模块: /Touchpoint_Advanced_Analysis/Firsttouch_report
#*程序: firsttouch_report_weekly.sh
#*功能: Weekly首触线索转化报表
#*开发人: Boyan XU00
#*开发日期: 2021-08-05
#*修改记录: 
#*          
#*********************************************************************/

pt=$3
pt_week_01=$(date -d "-1 day ${pt}" +'%YW%U')
b='W'
pt_week=${pt_week_01/W0/"$b"}
cur_week_start=$(date -d "${pt} -$(date -d "${pt}" +%u) days +1 day" +%Y%m%d)
cur_week_end=$(date -d "${pt} -$(date -d "${pt}" +%u) days +7 day" +%Y%m%d)
echo pt_week $pt_week
echo cur_week_start $cur_week_start
echo cur_week_end $cur_week_end

cd $(dirname $(readlink -f $0))
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  config.ini`

hive -hivevar queuename=$queuename --hivevar pt=$pt --hivevar pt_week=$pt_week --hivevar cur_week_start=$cur_week_start --hivevar cur_week_end=$cur_week_end -e "
set tez.queue.name=${queuename};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.groupby.position.alias=true;
set mapreduce.map.memory.mb=4096;

WITH calendar_df AS (

    SELECT
      day_key AS full_date,
      trim(concat(substr(clndr_wk_desc,3, 5), substr(clndr_wk_desc,9, 10))) as week
      FROM dtwarehouse.cdm_dim_calendar
    GROUP BY day_key, clndr_wk_desc

),

filtered_profile_df AS (

  SELECT
    mobile,
    fir_contact_week,
    fir_contact_date,
    fir_contact_series,
    mac_code,
    rfs_code,
    area,
    is_sec_net,
    activity_name,
    fir_contact_tp_id,
    brand
    from
    (
    SELECT
        mobile,
        fir_contact_week,
        from_unixtime(unix_timestamp(fir_contact_date),'yyyy-MM-dd hh:mm:ss') fir_contact_date,
        fir_contact_series,
        mac_code,
        rfs_code,
        area,
        is_sec_net,
        activity_name,
        fir_contact_tp_id,
        brand,
    row_number() over(partition by mobile,brand order by from_unixtime(unix_timestamp(fir_contact_date),'yyyy-MM-dd hh:mm:ss') desc) rank_num
    FROM marketing_modeling.app_touchpoints_profile_weekly
    WHERE regexp_replace(to_date(fir_contact_date), '-', '') <= '${pt}') a
    where rank_num=1
),

partitioned_profile_df AS (
SELECT
    mobile,
    fir_contact_week,
    from_unixtime(unix_timestamp(fir_contact_date),'yyyy-MM-dd hh:mm:ss') fir_contact_date,
    fir_contact_series,
    mac_code,
    rfs_code,
    area,
    is_sec_net,
    activity_name,
    fir_contact_tp_id,
    brand
    FROM marketing_modeling.app_touchpoints_profile_weekly
    where pt ='${pt_week}'



),

instore_df AS (

    SELECT
        raw_instore_df.action_week,
        filtered_profile_df.fir_contact_tp_id,
        filtered_profile_df.fir_contact_series,
        filtered_profile_df.mac_code,
        filtered_profile_df.rfs_code,
        filtered_profile_df.area,
        filtered_profile_df.is_sec_net,
        filtered_profile_df.activity_name,
        filtered_profile_df.brand,
        COUNT(DISTINCT raw_instore_df.mobile) AS instore_vol
    FROM
    (     SELECT
            t.mobile,
            t.brand,
            t.action_time,
            calendar_df.week AS action_week
        FROM
        (
            SELECT
            phone AS mobile,
            CASE
                WHEN detail['brand_id'] = '121' THEN 'MG'
                WHEN detail['brand_id'] = '101' THEN 'RW'
                ELSE ''
            END AS brand,
            cast(to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss') as string) AS action_time,
            date_format(to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss'), 'yyyyMM') AS action_month,
            pt
        from cdp.cdm_cdp_customer_behavior_detail
        WHERE pt >= '${cur_week_start}' AND pt <= '${cur_week_end}'
        and type='instore'
        and detail['dealer_code'] not like 'SR%'
        ) AS t
        LEFT JOIN calendar_df
        ON t.pt = calendar_df.full_date
    ) AS raw_instore_df
    LEFT JOIN filtered_profile_df
    ON raw_instore_df.mobile = filtered_profile_df.mobile AND raw_instore_df.brand = filtered_profile_df.brand
    WHERE
        filtered_profile_df.mobile IS NOT NULL

    GROUP BY raw_instore_df.action_week,
        filtered_profile_df.fir_contact_tp_id,
        filtered_profile_df.fir_contact_series,
        filtered_profile_df.mac_code,
        filtered_profile_df.rfs_code,
        filtered_profile_df.area,
        filtered_profile_df.is_sec_net,
        filtered_profile_df.activity_name,
        filtered_profile_df.brand

),

trial_df AS (

    SELECT
        raw_trial_df.action_week,
        filtered_profile_df.fir_contact_tp_id,
        filtered_profile_df.fir_contact_series,
        filtered_profile_df.mac_code,
        filtered_profile_df.rfs_code,
        filtered_profile_df.area,
        filtered_profile_df.is_sec_net,
        filtered_profile_df.activity_name,
        filtered_profile_df.brand,
        COUNT(DISTINCT raw_trial_df.mobile) AS trial_vol
    FROM
    (
        SELECT
            t.mobile,
            t.brand,
            t.action_time,
            calendar_df.week AS action_week
        FROM
        (
            select
            phone mobile,
            CASE
                WHEN detail['brand_id'] = '121' THEN 'MG'
                WHEN detail['brand_id'] = '101' THEN 'RW'
                ELSE ''
            END AS brand,
            cast(to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss') as string) AS action_time,
            pt
            from cdp.cdm_cdp_customer_behavior_detail where type='trial'
            and detail['dealer_code'] not like 'SR%'
            and pt >= '${cur_week_start}' AND pt <= '${cur_week_end}'
        ) AS t
        LEFT JOIN calendar_df
        ON t.pt = calendar_df.full_date
    ) AS raw_trial_df
    LEFT JOIN filtered_profile_df
    ON raw_trial_df.mobile = filtered_profile_df.mobile AND raw_trial_df.brand = filtered_profile_df.brand
    WHERE
        filtered_profile_df.mobile IS NOT NULL

    GROUP BY raw_trial_df.action_week,
        filtered_profile_df.fir_contact_tp_id,
        filtered_profile_df.fir_contact_series,
        filtered_profile_df.mac_code,
        filtered_profile_df.rfs_code,
        filtered_profile_df.area,
        filtered_profile_df.is_sec_net,
        filtered_profile_df.activity_name,
        filtered_profile_df.brand

),

raw_xianjiaoche AS (

    SELECT
        mobile,
        brand,
        action_time,
        vel_series_id
    FROM
    (
        SELECT
            get_vel_phone AS mobile,
            CASE
                WHEN brand_id = '10000000000220' THEN 'MG'
                WHEN brand_id = '10000000000086' THEN 'RW'
                ELSE ''
            END AS brand,
            create_time AS action_time,
            vel_series_id,
            oppor_id
        FROM dtwarehouse.ods_dlm_t_deliver_vel
        WHERE
            dealer_id IS NOT NULL
            AND create_user IS NOT NULL
            AND brand_id IN ('10000000000220', '10000000000086')
            AND regexp_replace(to_date(create_time), '-', '') BETWEEN '${cur_week_start}' AND '${cur_week_end}'
            AND pt = '${pt}'
    ) AS deliver_vel
    LEFT JOIN
    (
        SELECT oppor_id FROM dtwarehouse.ods_dlm_t_order_vhcl_relation
        WHERE pt = '${pt}' AND oppor_id IS NOT NULL
    ) AS order_vhcl_relation
    ON deliver_vel.oppor_id = order_vhcl_relation.oppor_id
    WHERE order_vhcl_relation.oppor_id IS NULL

),

xianjiaoche AS (  -- 取现交车车系id

    SELECT
        mobile,
        brand,
        CAST(action_time AS string) AS action_time,
        CAST(cdm_dim_series.series_id AS string) AS series_id
    FROM raw_xianjiaoche
    LEFT JOIN dtwarehouse.cdm_dim_series
    ON raw_xianjiaoche.vel_series_id = cdm_dim_series.series_dol_product_id

),

consume_behavior AS (

    SELECT
        phone AS mobile,
        CASE
            WHEN detail['brand_id'] = '121' THEN 'MG'
            WHEN detail['brand_id'] = '101' THEN 'RW'
            ELSE ''
        END AS brand,
       cast(to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss') as string) AS action_time,
        detail['series_id'] series_id
    FROM cdp.cdm_cdp_customer_behavior_detail
    WHERE pt >= '${cur_week_start}' AND pt <= '${cur_week_end}'
    and type ='consume'
    and detail['dealer_code'] not like 'SR%'),
app_dtksh_147_ as (
    select
    app_dtksh_147_.mobile,
    app_dtksh_147_.brand,
    app_dtksh_147_.action_time,
    cast(cdm_dim_series_.series_id as string) series_id
    from (
    select
    cust_tel as mobile,
    CASE
        WHEN chinese_name = 'MG' THEN 'MG'
        WHEN chinese_name = '荣威' THEN 'RW'
        ELSE ''
    END AS brand,
    ordercreatetime as action_time,
    dol_series_name
    from dtwarehouse.app_dtksh_147
    where rfs_name not like '%RWR%'
    and  pt='${pt}'
    AND regexp_replace(to_date(to_utc_timestamp(ordercreatetime,'yyyy-MM-dd HH:mm:ss')), '-', '') >= '${cur_week_start}'
    AND regexp_replace(to_date(to_utc_timestamp(ordercreatetime,'yyyy-MM-dd HH:mm:ss')), '-', '') <= '${cur_week_end}'
    ) app_dtksh_147_
    left join
    (select
    series_id,
    series_chinese_name
    from dtwarehouse.cdm_dim_series) cdm_dim_series_
    on app_dtksh_147_.dol_series_name = cdm_dim_series_.series_chinese_name
),
consume_df AS (

    SELECT
        raw_consume_df.action_week,
        filtered_profile_df.fir_contact_tp_id,
        raw_consume_df.series_id AS fir_contact_series, -- 替换首触车系为订单车系
        filtered_profile_df.mac_code,
        filtered_profile_df.rfs_code,
        filtered_profile_df.area,
        filtered_profile_df.is_sec_net,
        filtered_profile_df.activity_name,
        filtered_profile_df.brand,
        COUNT(DISTINCT raw_consume_df.mobile) AS consume_vol
    FROM
    (
        SELECT
            t.mobile,
            t.brand,
            t.action_time,
            calendar_df.week AS action_week,
            t.series_id
        FROM
        (
        SELECT
        *,
        row_number() over(partition by order_union.mobile,order_union.brand order by order_union.action_time) rank_
        from (
        SELECT * FROM app_dtksh_147_ ) order_union
        ) AS t
        LEFT JOIN calendar_df
        ON regexp_replace(to_date(t.action_time), '-', '') = calendar_df.full_date
    ) AS raw_consume_df
    LEFT JOIN filtered_profile_df
    ON raw_consume_df.mobile = filtered_profile_df.mobile AND raw_consume_df.brand = filtered_profile_df.brand
    WHERE
        filtered_profile_df.mobile IS NOT NULL

    GROUP BY raw_consume_df.action_week,
        filtered_profile_df.fir_contact_tp_id,
        raw_consume_df.series_id ,
        filtered_profile_df.mac_code,
        filtered_profile_df.rfs_code,
        filtered_profile_df.area,
        filtered_profile_df.is_sec_net,
        filtered_profile_df.activity_name,
        filtered_profile_df.brand

),

deliver_df AS (

    SELECT
        raw_deliver_df.action_week,
        filtered_profile_df.fir_contact_tp_id,
        raw_deliver_df.series_id AS fir_contact_series, -- 替换首触车系为交车车系
        filtered_profile_df.mac_code,
        filtered_profile_df.rfs_code,
        filtered_profile_df.area,
        filtered_profile_df.is_sec_net,
        filtered_profile_df.activity_name,
        filtered_profile_df.brand,
        COUNT(DISTINCT raw_deliver_df.mobile) AS deliver_vol
    FROM (
        SELECT
            t.mobile,
            t.brand,
            t.action_time,
            t.series_id,
            calendar_df.week AS action_week
        FROM
        (
            SELECT
                phone AS mobile,
                CASE
                    WHEN detail['brand_id'] = '121' THEN 'MG'
                    WHEN detail['brand_id'] = '101' THEN 'RW'
                    ELSE ''
                END AS brand,
                cast(to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss') as string) AS action_time,
                detail['series_id'] series_id
            FROM cdp.cdm_cdp_customer_behavior_detail
            WHERE pt >= '${cur_week_start}' AND pt <= '${cur_week_end}'
            and type = 'deliver'
            and detail['dealer_code'] not like 'SR%'
        ) AS t
        LEFT JOIN calendar_df
        ON regexp_replace(to_date(t.action_time), '-', '') = calendar_df.full_date
    ) AS raw_deliver_df
    LEFT JOIN filtered_profile_df
    ON raw_deliver_df.mobile = filtered_profile_df.mobile AND raw_deliver_df.brand = filtered_profile_df.brand
    WHERE
        filtered_profile_df.mobile IS NOT NULL

    GROUP BY raw_deliver_df.action_week,
        filtered_profile_df.fir_contact_tp_id,
        raw_deliver_df.series_id,
        filtered_profile_df.mac_code,
        filtered_profile_df.rfs_code,
        filtered_profile_df.area,
        filtered_profile_df.is_sec_net,
        filtered_profile_df.activity_name,
        filtered_profile_df.brand

),

grouped_profile_df AS (

    SELECT
        fir_contact_week,
        fir_contact_series,
        fir_contact_tp_id,
        mac_code,
        rfs_code,
        area,
        is_sec_net,
        activity_name,
        brand,
        COUNT(mobile) AS cust_vol
    FROM partitioned_profile_df
    GROUP BY fir_contact_week,
        fir_contact_series,
        fir_contact_tp_id,
        mac_code,
        rfs_code,
        area,
        is_sec_net,
        activity_name,
        brand
),

profile_df as (
select
distinct
*
from (
select
fir_contact_week,
fir_contact_tp_id,
fir_contact_series,
mac_code,
rfs_code,
area,
is_sec_net,
activity_name,
brand
from
grouped_profile_df
UNION DISTINCT
select
action_week fir_contact_week,
fir_contact_tp_id,
fir_contact_series,
mac_code,
rfs_code,
area,
is_sec_net,
activity_name,
brand
from
instore_df
UNION DISTINCT
select
action_week fir_contact_week,
fir_contact_tp_id,
fir_contact_series,
mac_code,
rfs_code,
area,
is_sec_net,
activity_name,
brand
from
trial_df
UNION DISTINCT
select
action_week fir_contact_week,
fir_contact_tp_id,
fir_contact_series,
mac_code,
rfs_code,
area,
is_sec_net,
activity_name,
brand
from
consume_df
UNION DISTINCT
select
action_week fir_contact_week,
fir_contact_tp_id,
fir_contact_series,
mac_code,
rfs_code,
area,
is_sec_net,
activity_name,
brand
from
deliver_df ) tmp
)
,
final_df AS (
SELECT
    profile_df.fir_contact_week,
    profile_df.fir_contact_series,
    profile_df.fir_contact_tp_id,
    profile_df.mac_code,
    profile_df.rfs_code,
    profile_df.area,
    profile_df.is_sec_net,
    profile_df.activity_name,
    profile_df.brand,
    nvl(grouped_profile_df.cust_vol, 0) as cust_vol, -- 全量首触人数
    nvl(instore_df.instore_vol, 0) AS instore_vol,  -- 到店人数
    nvl(trial_df.trial_vol, 0) AS trial_vol, -- 试驾人数
    nvl(consume_df.consume_vol, 0) AS consume_vol, -- 订单人数(含交现车)
    nvl(deliver_df.deliver_vol, 0) AS deliver_vol -- 交车人数
from profile_df
left join grouped_profile_df
on
    profile_df.fir_contact_week = grouped_profile_df.fir_contact_week AND
    profile_df.fir_contact_tp_id = grouped_profile_df.fir_contact_tp_id AND
    profile_df.fir_contact_series = grouped_profile_df.fir_contact_series AND
    profile_df.area = grouped_profile_df.area AND
    profile_df.mac_code = grouped_profile_df.mac_code AND
    profile_df.rfs_code = grouped_profile_df.rfs_code AND
    profile_df.is_sec_net = grouped_profile_df.is_sec_net AND
    profile_df.activity_name = grouped_profile_df.activity_name AND
    profile_df.brand = grouped_profile_df.brand
LEFT JOIN instore_df
ON
    profile_df.fir_contact_week = instore_df.action_week AND
    profile_df.fir_contact_tp_id = instore_df.fir_contact_tp_id AND
    profile_df.fir_contact_series = instore_df.fir_contact_series AND
    profile_df.area = instore_df.area AND
    profile_df.mac_code = instore_df.mac_code AND
    profile_df.rfs_code = instore_df.rfs_code AND
    profile_df.is_sec_net = instore_df.is_sec_net AND
    profile_df.activity_name = instore_df.activity_name AND
    profile_df.brand = instore_df.brand
LEFT JOIN trial_df
ON
    profile_df.fir_contact_week = trial_df.action_week AND
    profile_df.fir_contact_tp_id = trial_df.fir_contact_tp_id AND
    profile_df.fir_contact_series = trial_df.fir_contact_series AND
    profile_df.area = trial_df.area AND
    profile_df.mac_code = trial_df.mac_code AND
    profile_df.rfs_code = trial_df.rfs_code AND
    profile_df.is_sec_net = trial_df.is_sec_net AND
    profile_df.activity_name = trial_df.activity_name AND
    profile_df.brand = trial_df.brand
LEFT JOIN consume_df
ON
    profile_df.fir_contact_week = consume_df.action_week AND
    profile_df.fir_contact_tp_id = consume_df.fir_contact_tp_id AND
    profile_df.fir_contact_series = consume_df.fir_contact_series AND
    profile_df.area = consume_df.area AND
    profile_df.mac_code = consume_df.mac_code AND
    profile_df.rfs_code = consume_df.rfs_code AND
    profile_df.is_sec_net = consume_df.is_sec_net AND
    profile_df.activity_name = consume_df.activity_name AND
    profile_df.brand = consume_df.brand
LEFT JOIN deliver_df
ON
    profile_df.fir_contact_week = deliver_df.action_week AND
    profile_df.fir_contact_tp_id = deliver_df.fir_contact_tp_id AND
    profile_df.fir_contact_series = deliver_df.fir_contact_series AND
    profile_df.area = deliver_df.area AND
    profile_df.mac_code = deliver_df.mac_code AND
    profile_df.rfs_code = deliver_df.rfs_code AND
    profile_df.is_sec_net = deliver_df.is_sec_net AND
    profile_df.activity_name = deliver_df.activity_name AND
    profile_df.brand = deliver_df.brand
)


INSERT overwrite TABLE marketing_modeling.app_fir_contact_conversion_report_weekly_a PARTITION (pt)
SELECT
    mac_code,
    rfs_code,
    area,
    is_sec_net,
    activity_name,
    fir_contact_tp_id,
    fir_contact_series,
    fir_contact_week,
    cast(cust_vol AS BIGINT) cust_vol,
    instore_vol,
    trial_vol,
    consume_vol,
    deliver_vol,
    brand,
    regexp_replace(fir_contact_week, ' ', '') as pt
FROM final_df
"
