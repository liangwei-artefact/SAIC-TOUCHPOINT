pt1=$3
pt2=$4

hive --hivevar pt1=$pt1 --hivevar pt2=$pt2 -e "
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=2048;
SET hive.exec.max.dynamic.partitions.pernode=2000;
SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb=8192;
INSERT OVERWRITE TABLE marketing_modeling.cdm_ts_register_i PARTITION(pt,brand)
SELECT * FROM
(SELECT cellphone AS mobile,
       regist_date AS action_time,
       CASE
           WHEN regist_channel = 'PORTAL' AND brand_code = 2 THEN '002001001000_tp'
           WHEN regist_channel = 'APP' AND brand_code = 2 THEN '002001002000_tp'
           WHEN regist_channel = 'MINI PROGRAM' AND brand_code = 2 THEN '002001003001_tp'
           WHEN regist_channel = 'ALIPAY' AND brand_code = 2 THEN '002001003002_tp'
           WHEN regist_channel = 'ZEBRA' AND brand_code = 2 THEN '002001003003_tp'
           WHEN regist_channel = 'QUICK_APP' AND brand_code = 2 THEN '002001003004_tp'
           WHEN regist_channel = 'TOUTIAO' AND brand_code = 2 THEN '002001003005_tp'
           WHEN regist_channel = 'BAIDU' AND brand_code = 2 THEN '002001003006_tp'
           WHEN regist_channel = 'KWAI' AND brand_code = 2 THEN '002001003007_tp'
           WHEN regist_channel = 'OLD_BELT_NEW' AND brand_code = 2 THEN '002001004001_tp'
           WHEN regist_channel = 'SPRING_FEATIVAL_FISSION' AND brand_code = 2 THEN '002001004002_tp'
           WHEN regist_channel = 'CCM' AND brand_code = 2 THEN '002001005000_tp'

           WHEN regist_channel = 'PORTAL' AND brand_code = 1 THEN '002001001000_rw'
           WHEN regist_channel = 'APP'AND brand_code = 1 THEN '002001002000_rw'
           WHEN regist_channel = 'MINI PROGRAM' AND brand_code = 1 THEN '002001003001_rw'
           WHEN regist_channel = 'ALIPAY' AND brand_code = 1 THEN '002001003002_rw'
           WHEN regist_channel = 'ZEBRA' AND brand_code = 1 THEN '002001003003_rw'
           WHEN regist_channel = 'QUICK_APP' AND brand_code = 1 THEN '002001003004_rw'
           WHEN regist_channel = 'TOUTIAO' AND brand_code = 1 THEN '002001003005_rw'
           WHEN regist_channel = 'BAIDU' AND brand_code = 1 THEN '002001003006_rw'
           WHEN regist_channel = 'KWAI' AND brand_code = 1 THEN '002001003007_rw'
           WHEN regist_channel = 'OLD_BELT_NEW' AND brand_code = 1 THEN '002001004001_rw'
           WHEN regist_channel = 'SPRING_FEATIVAL_FISSION' AND brand_code = 1 THEN '002001004002_rw'
           WHEN regist_channel = 'CCM' AND brand_code = 1 THEN '002001005000_rw'
       END AS touchpoint_id,
       CASE
           WHEN brand_code = 2 THEN 'MG'
           WHEN brand_code = 1 THEN 'RW'
           ELSE NULL
       END AS brand,
       date_format(regist_date,'yyyyMMdd') as pt
FROM dtwarehouse.ods_ccm_member
WHERE 
    pt = ${pt2} 
    and status = 1
    and regexp_replace(to_date(regist_date), '-', '') >= ${pt1} and regexp_replace(to_date(regist_date), '-', '') <= ${pt2} 
) t1
WHERE
    mobile regexp '^[1][3-9][0-9]{9}$'
    AND action_time IS NOT NULL
    AND touchpoint_id IS NOT NULL
	AND brand IS NOT NULL
"
