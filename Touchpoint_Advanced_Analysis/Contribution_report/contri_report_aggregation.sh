#!/bin/bash
#/*********************************************************************
#*模块: /Touchpoint_Advanced_Analysis/Contribution_Report
#*程序: contri_report_aggregation.sh
#*功能: 计算触点转化
#*开发人: Boyan XU
#*开发日: 2021-09-05
#*修改记录:
#*
#*********************************************************************/

brand=$1
cd $(dirname $(readlink -f $0))
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  config.ini`

hive -hivevar queuename=queuename --hivevar brand=$brand -e "
set tez.queue.name=${queuename};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.groupby.position.alias=true;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.exec.max.dynamic.partitions=2048;
set hive.exec.max.dynamic.partitions.pernode=1000;

WITH raw_final_df AS (
    SELECT * FROM marketing_modeling.app_tmp_tp_asset_report_a
),

agged_profile_df AS (

    SELECT
        fir_contact_month,
        fir_contact_tp_id,
        fir_contact_series,
        mac_code,
        rfs_code,
        area,
        cust_vol
    FROM marketing_modeling.app_tmp_agged_profile

),

undeal_report_df AS (

    SELECT
        fir_contact_month,
        fir_contact_tp_id,
        fir_contact_series,
        mac_code,
        rfs_code,
        area,
        undeal_vol
    FROM marketing_modeling.app_tmp_undeal_report_a

),

lv1_ranked_tp_profile_df AS (

    SELECT
        mobile,
        fir_contact_month,
        fir_contact_date,
        fir_contact_series,
        mac_code,
        rfs_code,
        area,
        fir_contact_tp_id,
        level_1_tp_id AS tp_id,
        action_time,
        instore_flag,
        trial_flag,
        consume_flag,
        nvl(undeal_flag, 0) AS undeal_flag,
        nvl(exit_flag, 0) AS exit_flag,
        row_number() over (PARTITION BY mobile, fir_contact_tp_id, fir_contact_month, level_1_tp_id ORDER BY action_time DESC) AS rank_num
    FROM raw_final_df

),

lv1_distinct_flag_df AS (

    SELECT
        *,
        CASE
            WHEN rank_num = 1 THEN 1
            ELSE 0
        END AS distinct_flag
    FROM lv1_ranked_tp_profile_df

),

lv1_raw_final_df AS (

    SELECT
        fir_contact_month,
        fir_contact_tp_id,
        fir_contact_series,
        mac_code,
        rfs_code,
        area,
        tp_id,
        count(mobile) AS tp_pv,
        sum(distinct_flag) AS tp_uv,
        sum(exit_flag * undeal_flag) AS exit_pv,
        sum(distinct_flag * instore_flag) AS instore_vol,
        sum(distinct_flag * trial_flag) AS trial_vol,
        sum(distinct_flag * consume_flag) AS consume_vol
    FROM lv1_distinct_flag_df
    GROUP BY 1,2,3,4,5,6,7

),

lv1_final_df AS (

    SELECT
		lv1_raw_final_df.tp_id AS touchpoint_id,
        lv1_raw_final_df.fir_contact_month,
        lv1_raw_final_df.fir_contact_tp_id,
        lv1_raw_final_df.fir_contact_series,
        lv1_raw_final_df.mac_code,
        lv1_raw_final_df.rfs_code,
        lv1_raw_final_df.area,
        lv1_raw_final_df.tp_pv,
        lv1_raw_final_df.tp_uv,
        lv1_raw_final_df.instore_vol,
        lv1_raw_final_df.trial_vol,
        lv1_raw_final_df.consume_vol,
        agged_profile_df.cust_vol,
		lv1_raw_final_df.exit_pv,
		undeal_report_df.undeal_vol,
        lv1_raw_final_df.tp_uv / agged_profile_df.cust_vol AS tp_coverage,
        lv1_raw_final_df.tp_pv / lv1_raw_final_df.tp_uv AS tp_avg_times,
		lv1_raw_final_df.exit_pv / undeal_report_df.undeal_vol AS exit_rate,
        lv1_raw_final_df.instore_vol / lv1_raw_final_df.tp_uv AS tp_instore_rate,
        lv1_raw_final_df.trial_vol / lv1_raw_final_df.tp_uv AS tp_trial_rate,
        lv1_raw_final_df.consume_vol / lv1_raw_final_df.tp_uv AS tp_deal_rate,
		lv1_raw_final_df.fir_contact_month AS pt,
		'${brand}' AS brand
    FROM lv1_raw_final_df
    LEFT JOIN agged_profile_df
	ON
        lv1_raw_final_df.fir_contact_month = agged_profile_df.fir_contact_month AND
        lv1_raw_final_df.fir_contact_tp_id = agged_profile_df.fir_contact_tp_id AND
        lv1_raw_final_df.fir_contact_series = agged_profile_df.fir_contact_series AND
        lv1_raw_final_df.area = agged_profile_df.area AND
        lv1_raw_final_df.mac_code = agged_profile_df.mac_code AND
        lv1_raw_final_df.rfs_code = agged_profile_df.rfs_code
    LEFT JOIN undeal_report_df
	ON
        lv1_raw_final_df.fir_contact_month = undeal_report_df.fir_contact_month AND
        lv1_raw_final_df.fir_contact_tp_id = undeal_report_df.fir_contact_tp_id AND
        lv1_raw_final_df.fir_contact_series = undeal_report_df.fir_contact_series AND
        lv1_raw_final_df.area = undeal_report_df.area AND
        lv1_raw_final_df.mac_code = undeal_report_df.mac_code AND
        lv1_raw_final_df.rfs_code = undeal_report_df.rfs_code

),

lv2_ranked_tp_profile_df AS (

    SELECT
        mobile,
        fir_contact_month,
        fir_contact_date,
        fir_contact_series,
        mac_code,
        rfs_code,
        area,
        fir_contact_tp_id,
        level_2_tp_id AS tp_id,
        action_time,
        instore_flag,
        trial_flag,
        consume_flag,
        nvl(undeal_flag, 0) AS undeal_flag,
        nvl(exit_flag, 0) AS exit_flag,
        ROW_NUMBER() over (PARTITION BY mobile, fir_contact_tp_id, fir_contact_month, level_2_tp_id ORDER BY action_time DESC) AS rank_num
    FROM raw_final_df

),

lv2_distinct_flag_df AS (

    SELECT
        *,
        CASE
            WHEN rank_num = 1 THEN 1
            ELSE 0
        END AS distinct_flag
    FROM lv2_ranked_tp_profile_df

),

lv2_raw_final_df AS (

    SELECT
        fir_contact_month,
        fir_contact_tp_id,
        fir_contact_series,
        mac_code,
        rfs_code,
        area,
        tp_id,
        count(mobile) AS tp_pv,
        sum(distinct_flag) AS tp_uv,
        sum(exit_flag * undeal_flag) AS exit_pv,
        sum(distinct_flag * instore_flag) AS instore_vol,
        sum(distinct_flag * trial_flag) AS trial_vol,
        sum(distinct_flag * consume_flag) AS consume_vol
    FROM lv2_distinct_flag_df
    GROUP BY 1,2,3,4,5,6,7

),

lv2_final_df AS (

    SELECT
		lv2_raw_final_df.tp_id AS touchpoint_id,
        lv2_raw_final_df.fir_contact_month,
        lv2_raw_final_df.fir_contact_tp_id,
        lv2_raw_final_df.fir_contact_series,
        lv2_raw_final_df.mac_code,
        lv2_raw_final_df.rfs_code,
        lv2_raw_final_df.area,
        lv2_raw_final_df.tp_pv,
        lv2_raw_final_df.tp_uv,
        lv2_raw_final_df.instore_vol,
        lv2_raw_final_df.trial_vol,
        lv2_raw_final_df.consume_vol,
        agged_profile_df.cust_vol,
		lv2_raw_final_df.exit_pv,
		undeal_report_df.undeal_vol,
        lv2_raw_final_df.tp_uv / agged_profile_df.cust_vol AS tp_coverage,
        lv2_raw_final_df.tp_pv / lv2_raw_final_df.tp_uv AS tp_avg_times,
		lv2_raw_final_df.exit_pv / undeal_report_df.undeal_vol AS exit_rate,
        lv2_raw_final_df.instore_vol / lv2_raw_final_df.tp_uv AS tp_instore_rate,
        lv2_raw_final_df.trial_vol / lv2_raw_final_df.tp_uv AS tp_trial_rate,
        lv2_raw_final_df.consume_vol / lv2_raw_final_df.tp_uv AS tp_deal_rate,
		lv2_raw_final_df.fir_contact_month AS pt,
		'${brand}' AS brand
    FROM lv2_raw_final_df
    LEFT JOIN agged_profile_df
	ON
        lv2_raw_final_df.fir_contact_month = agged_profile_df.fir_contact_month AND
        lv2_raw_final_df.fir_contact_tp_id = agged_profile_df.fir_contact_tp_id AND
        lv2_raw_final_df.fir_contact_series = agged_profile_df.fir_contact_series AND
        lv2_raw_final_df.area = agged_profile_df.area AND
        lv2_raw_final_df.mac_code = agged_profile_df.mac_code AND
        lv2_raw_final_df.rfs_code = agged_profile_df.rfs_code
    LEFT JOIN undeal_report_df
	ON
        lv2_raw_final_df.fir_contact_month = undeal_report_df.fir_contact_month AND
        lv2_raw_final_df.fir_contact_tp_id = undeal_report_df.fir_contact_tp_id AND
        lv2_raw_final_df.fir_contact_series = undeal_report_df.fir_contact_series AND
        lv2_raw_final_df.area = undeal_report_df.area AND
        lv2_raw_final_df.mac_code = undeal_report_df.mac_code AND
        lv2_raw_final_df.rfs_code = undeal_report_df.rfs_code
),

lv3_ranked_tp_profile_df AS (

    SELECT
        mobile,
        fir_contact_month,
        fir_contact_date,
        fir_contact_series,
        mac_code,
        rfs_code,
        area,
        fir_contact_tp_id,
        level_3_tp_id AS tp_id,
        action_time,
        instore_flag,
        trial_flag,
        consume_flag,
        nvl(undeal_flag, 0) AS undeal_flag,
        nvl(exit_flag, 0) AS exit_flag,
        ROW_NUMBER() over (PARTITION BY mobile, fir_contact_tp_id, fir_contact_month, level_3_tp_id ORDER BY action_time DESC) AS rank_num
    FROM raw_final_df

),

lv3_distinct_flag_df AS (

    SELECT
        *,
        CASE
            WHEN rank_num = 1 THEN 1
            ELSE 0
        END AS distinct_flag
    FROM lv3_ranked_tp_profile_df

),

lv3_raw_final_df AS (

    SELECT
        fir_contact_month,
        fir_contact_tp_id,
        fir_contact_series,
        mac_code,
        rfs_code,
        area,
        tp_id,
        count(mobile) AS tp_pv,
        sum(distinct_flag) AS tp_uv,
        sum(exit_flag * undeal_flag) AS exit_pv,
        sum(distinct_flag * instore_flag) AS instore_vol,
        sum(distinct_flag * trial_flag) AS trial_vol,
        sum(distinct_flag * consume_flag) AS consume_vol
    FROM lv3_distinct_flag_df
    GROUP BY 1,2,3,4,5,6,7

),

lv3_final_df AS (

    SELECT
		lv3_raw_final_df.tp_id AS touchpoint_id,
        lv3_raw_final_df.fir_contact_month,
        lv3_raw_final_df.fir_contact_tp_id,
        lv3_raw_final_df.fir_contact_series,
        lv3_raw_final_df.mac_code,
        lv3_raw_final_df.rfs_code,
        lv3_raw_final_df.area,
        lv3_raw_final_df.tp_pv,
        lv3_raw_final_df.tp_uv,
        lv3_raw_final_df.instore_vol,
        lv3_raw_final_df.trial_vol,
        lv3_raw_final_df.consume_vol,
        agged_profile_df.cust_vol,
		lv3_raw_final_df.exit_pv,
		undeal_report_df.undeal_vol,
        lv3_raw_final_df.tp_uv / agged_profile_df.cust_vol AS tp_coverage,
        lv3_raw_final_df.tp_pv / lv3_raw_final_df.tp_uv AS tp_avg_times,
		lv3_raw_final_df.exit_pv / undeal_report_df.undeal_vol AS exit_rate,
        lv3_raw_final_df.instore_vol / lv3_raw_final_df.tp_uv AS tp_instore_rate,
        lv3_raw_final_df.trial_vol / lv3_raw_final_df.tp_uv AS tp_trial_rate,
        lv3_raw_final_df.consume_vol / lv3_raw_final_df.tp_uv AS tp_deal_rate,
		lv3_raw_final_df.fir_contact_month AS pt,
		'${brand}' AS brand
    FROM lv3_raw_final_df
    LEFT JOIN agged_profile_df
	ON
        lv3_raw_final_df.fir_contact_month = agged_profile_df.fir_contact_month AND
        lv3_raw_final_df.fir_contact_tp_id = agged_profile_df.fir_contact_tp_id AND
        lv3_raw_final_df.fir_contact_series = agged_profile_df.fir_contact_series AND
        lv3_raw_final_df.area = agged_profile_df.area AND
        lv3_raw_final_df.mac_code = agged_profile_df.mac_code AND
        lv3_raw_final_df.rfs_code = agged_profile_df.rfs_code
    LEFT JOIN undeal_report_df
	ON
        lv3_raw_final_df.fir_contact_month = undeal_report_df.fir_contact_month AND
        lv3_raw_final_df.fir_contact_tp_id = undeal_report_df.fir_contact_tp_id AND
        lv3_raw_final_df.fir_contact_series = undeal_report_df.fir_contact_series AND
        lv3_raw_final_df.area = undeal_report_df.area AND
        lv3_raw_final_df.mac_code = undeal_report_df.mac_code AND
        lv3_raw_final_df.rfs_code = undeal_report_df.rfs_code

),

lv4_ranked_tp_profile_df AS (

    SELECT
        mobile,
        fir_contact_month,
        fir_contact_date,
        fir_contact_series,
        mac_code,
        rfs_code,
        area,
        fir_contact_tp_id,
        level_4_tp_id AS tp_id,
        action_time,
        instore_flag,
        trial_flag,
        consume_flag,
        nvl(undeal_flag, 0) AS undeal_flag,
        nvl(exit_flag, 0) AS exit_flag,
        row_number() over (PARTITION BY mobile, fir_contact_tp_id, fir_contact_month, level_4_tp_id ORDER BY action_time DESC) AS rank_num
    FROM raw_final_df

),

lv4_distinct_flag_df AS (

    SELECT
        *,
        CASE
            WHEN rank_num = 1 THEN 1
            ELSE 0
        END AS distinct_flag
    FROM lv4_ranked_tp_profile_df

),

lv4_raw_final_df AS (

    SELECT
        fir_contact_month,
        fir_contact_tp_id,
        fir_contact_series,
        mac_code,
        rfs_code,
        area,
        tp_id,
        count(mobile) AS tp_pv,
        sum(distinct_flag) AS tp_uv,
        sum(exit_flag * undeal_flag) AS exit_pv,
        sum(distinct_flag * instore_flag) AS instore_vol,
        sum(distinct_flag * trial_flag) AS trial_vol,
        sum(distinct_flag * consume_flag) AS consume_vol
    FROM lv4_distinct_flag_df
    GROUP BY 1,2,3,4,5,6,7

),

lv4_final_df AS (

    SELECT
		lv4_raw_final_df.tp_id AS touchpoint_id,
        lv4_raw_final_df.fir_contact_month,
        lv4_raw_final_df.fir_contact_tp_id,
        lv4_raw_final_df.fir_contact_series,
        lv4_raw_final_df.mac_code,
        lv4_raw_final_df.rfs_code,
        lv4_raw_final_df.area,
        lv4_raw_final_df.tp_pv,
        lv4_raw_final_df.tp_uv,
        lv4_raw_final_df.instore_vol,
        lv4_raw_final_df.trial_vol,
        lv4_raw_final_df.consume_vol,
        agged_profile_df.cust_vol,
		lv4_raw_final_df.exit_pv,
		undeal_report_df.undeal_vol,
        lv4_raw_final_df.tp_uv / agged_profile_df.cust_vol AS tp_coverage,
        lv4_raw_final_df.tp_pv / lv4_raw_final_df.tp_uv AS tp_avg_times,
		lv4_raw_final_df.exit_pv / undeal_report_df.undeal_vol AS exit_rate,
        lv4_raw_final_df.instore_vol / lv4_raw_final_df.tp_uv AS tp_instore_rate,
        lv4_raw_final_df.trial_vol / lv4_raw_final_df.tp_uv AS tp_trial_rate,
        lv4_raw_final_df.consume_vol / lv4_raw_final_df.tp_uv AS tp_deal_rate,
		lv4_raw_final_df.fir_contact_month AS pt,
		'${brand}' AS brand
    FROM lv4_raw_final_df
    LEFT JOIN agged_profile_df
	ON
        lv4_raw_final_df.fir_contact_month = agged_profile_df.fir_contact_month AND
        lv4_raw_final_df.fir_contact_tp_id = agged_profile_df.fir_contact_tp_id AND
        lv4_raw_final_df.fir_contact_series = agged_profile_df.fir_contact_series AND
        lv4_raw_final_df.area = agged_profile_df.area AND
        lv4_raw_final_df.mac_code = agged_profile_df.mac_code AND
        lv4_raw_final_df.rfs_code = agged_profile_df.rfs_code
    LEFT JOIN undeal_report_df
	ON
        lv4_raw_final_df.fir_contact_month = undeal_report_df.fir_contact_month AND
        lv4_raw_final_df.fir_contact_tp_id = undeal_report_df.fir_contact_tp_id AND
        lv4_raw_final_df.fir_contact_series = undeal_report_df.fir_contact_series AND
        lv4_raw_final_df.area = undeal_report_df.area AND
        lv4_raw_final_df.mac_code = undeal_report_df.mac_code AND
        lv4_raw_final_df.rfs_code = undeal_report_df.rfs_code
)

INSERT OVERWRITE TABLE marketing_modeling.app_tp_asset_report_a PARTITION(pt, brand)
SELECT 1 AS touchpoint_level, * FROM lv1_final_df
UNION ALL
SELECT 2 AS touchpoint_level, * FROM lv2_final_df
UNION ALL
SELECT 3 AS touchpoint_level, * FROM lv3_final_df
UNION ALL
SELECT 4 AS touchpoint_level, * FROM lv4_final_df
"
