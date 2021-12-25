#!/bin/bash
pt2=$3
pre_day=$4
pt1=$(date -d "${pt2} -$pre_day day" '+%Y%m%d')
cd $(dirname $(readlink -f $0))
queue_name=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  config.ini`
hive --hivevar pt1=$pt1 --hivevar pt2=$pt2 --hivevar queue_name=${queue_name} -e "
set tez.queue.name=${queue_name};
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
SET hive.exec.max.dynamic.partitions=2048;
SET hive.exec.max.dynamic.partitions.pernode=2000;
SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb=8192;
INSERT OVERWRITE TABLE marketing_modeling.cdm_ts_register_i PARTITION(pt,brand)
SELECT
 mobile,
action_time,
touchpoint_id,
cast(pt as string) pt,
cast(brand as string) brand
 FROM
(SELECT phone AS mobile,
       detail['behavior_time'] AS action_time,
       CASE
           WHEN detail['regist_channel'] = 'PORTAL' AND detail['brand_code'] = 2 THEN '002001001000_tp'
           WHEN detail['regist_channel'] = 'APP' AND detail['brand_code'] = 2 THEN '002001002000_tp'
           WHEN detail['regist_channel'] = 'MINI PROGRAM' AND detail['brand_code'] = 2 THEN '002001003001_tp'
           WHEN detail['regist_channel'] = 'ALIPAY' AND detail['brand_code'] = 2 THEN '002001003002_tp'
           WHEN detail['regist_channel'] = 'ZEBRA' AND detail['brand_code'] = 2 THEN '002001003003_tp'
           WHEN detail['regist_channel'] = 'QUICK_APP' AND detail['brand_code'] = 2 THEN '002001003004_tp'
           WHEN detail['regist_channel'] = 'TOUTIAO' AND detail['brand_code'] = 2 THEN '002001003005_tp'
           WHEN detail['regist_channel'] = 'BAIDU' AND detail['brand_code'] = 2 THEN '002001003006_tp'
           WHEN detail['regist_channel'] = 'KWAI' AND detail['brand_code'] = 2 THEN '002001003007_tp'
           WHEN detail['regist_channel'] = 'OLD_BELT_NEW' AND detail['brand_code'] = 2 THEN '002001004001_tp'
           WHEN detail['regist_channel'] = 'SPRING_FEATIVAL_FISSION' AND detail['brand_code'] = 2 THEN '002001004002_tp'
           WHEN detail['regist_channel'] = 'CCM' AND detail['brand_code'] = 2 THEN '002001005000_tp'

           WHEN detail['regist_channel'] = 'PORTAL' AND detail['brand_code'] = 1 THEN '002001001000_rw'
           WHEN detail['regist_channel'] = 'APP'AND detail['brand_code'] = 1 THEN '002001002000_rw'
           WHEN detail['regist_channel'] = 'MINI PROGRAM' AND detail['brand_code'] = 1 THEN '002001003001_rw'
           WHEN detail['regist_channel'] = 'ALIPAY' AND detail['brand_code'] = 1 THEN '002001003002_rw'
           WHEN detail['regist_channel'] = 'ZEBRA' AND detail['brand_code'] = 1 THEN '002001003003_rw'
           WHEN detail['regist_channel'] = 'QUICK_APP' AND detail['brand_code'] = 1 THEN '002001003004_rw'
           WHEN detail['regist_channel'] = 'TOUTIAO' AND detail['brand_code'] = 1 THEN '002001003005_rw'
           WHEN detail['regist_channel'] = 'BAIDU' AND detail['brand_code'] = 1 THEN '002001003006_rw'
           WHEN detail['regist_channel'] = 'KWAI' AND detail['brand_code'] = 1 THEN '002001003007_rw'
           WHEN detail['regist_channel'] = 'OLD_BELT_NEW' AND detail['brand_code'] = 1 THEN '002001004001_rw'
           WHEN detail['regist_channel'] = 'SPRING_FEATIVAL_FISSION' AND detail['brand_code'] = 1 THEN '002001004002_rw'
           WHEN detail['regist_channel'] = 'CCM' AND detail['brand_code'] = 1 THEN '002001005000_rw'
       END AS touchpoint_id,
       CASE
           WHEN detail['brand_code'] = 2 THEN 'MG'
           WHEN detail['brand_code'] = 1 THEN 'RW'
           ELSE NULL
       END AS brand,
       date_format(to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') as pt
-- FROM dtwarehouse.ods_ccm_member
    from
      cdp.cdm_cdp_customer_behavior_detail
WHERE
    pt = ${pt2}
    and type = 'app_regist'
    -- and detail['status'] = 1
    and regexp_replace(to_date(to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss')), '-', '') >= ${pt1} and regexp_replace(to_date(to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss')), '-', '') <= ${pt2}
) t1
WHERE
    mobile regexp '^[1][3-9][0-9]{9}$'
    AND action_time IS NOT NULL
    AND touchpoint_id IS NOT NULL
	AND brand IS NOT NULL
"
