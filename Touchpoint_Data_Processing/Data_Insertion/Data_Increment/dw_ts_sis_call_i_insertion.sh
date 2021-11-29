pt=$3
hive --hivevar pt=$pt -e "set hive.exec.dynamic.partition.mode=nonstrict;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
SET hive.exec.max.dynamic.partitions=2048;
SET hive.exec.max.dynamic.partitions.pernode=1000;
INSERT OVERWRITE TABLE marketing_modeling.dw_ts_sis_call_i PARTITION (pt,brand)
SELECT * FROM
(SELECT phone AS mobile,
       cast(from_unixtime(unix_timestamp(cast(detail['begin_time'] as string), 'yyyyMMddHHmmss')) as TIMESTAMP) as action_time,
       CASE
           WHEN detail['ob_result_code'] in ('003','004','005','006','009','010') and (detail['brand'] like '%MG%' or detail['brand'] like '%名爵%') THEN '002005002000_tp'
           WHEN detail['talk_length'] > 0.0 and detail['talk_length'] < 10 and (detail['brand'] like '%MG%' or detail['brand'] like '%名爵%') THEN '002005001001_tp'
           WHEN detail['talk_length'] >= 10 and detail['talk_length'] < 30 and (detail['brand'] like '%MG%' or detail['brand'] like '%名爵%') THEN '002005001002_tp'
           WHEN detail['talk_length'] >= 30 and detail['talk_length'] <60 and (detail['brand'] like '%MG%' or detail['brand'] like '%名爵%') THEN '002005001003_tp'
           WHEN detail['talk_length'] >= 60 and (detail['brand'] like '%MG%' or detail['brand'] like '%名爵%') THEN '002011001004_tp'
           WHEN detail['brand'] like '%MG%' or detail['brand'] like '%名爵%' THEN '002011002000_tp'
           WHEN detail['ob_result_code'] in ('003','004','005','006','009','010') and (detail['brand'] like '%ROEWE%' or detail['brand'] like '%荣威%') THEN '002005001000_rw'
           WHEN detail['talk_length'] > 0.0 and detail['talk_length'] < 10 and (detail['brand'] like '%ROEWE%' or detail['brand'] like '%荣威%') THEN '002005001001_rw'
           WHEN detail['talk_length'] >= 10 and detail['talk_length'] < 30 and (detail['brand'] like '%ROEWE%' or detail['brand'] like '%荣威%') THEN '002005001002_rw'
           WHEN detail['talk_length'] >= 30 and detail['talk_length'] <60 and (detail['brand'] like '%ROEWE%' or detail['brand'] like '%荣威%') THEN '002005001003_rw'
           WHEN detail['talk_length'] >= 60 and detail['brand'] like '%ROEWE%' or detail['brand'] like '%荣威%' THEN '002005001004_rw'
           WHEN detail['brand'] like '%ROEWE%' or detail['brand'] like '%荣威%' THEN '002005002000_rw'
       END AS touchpoint_id,
       pt,
       CASE
           WHEN detail['brand'] like '%MG%' THEN 'MG'
           WHEN detail['brand'] like '%名爵%' THEN 'MG'
           WHEN detail['brand'] like '%ROEWE%' THEN 'RW'
           WHEN detail['brand'] like '%荣威%' THEN 'RW'
       END AS brand
FROM cdp.cdm_cdp_customer_behavior_detail
WHERE TYPE = 'sis_call' AND detail['task_type'] in ('001','002')
AND pt >= ${pt}
) t1
WHERE
    mobile regexp '^[1][3-9][0-9]{9}$'
    AND action_time IS NOT NULL
    AND touchpoint_id IS NOT NULL
	AND brand IS NOT NULL
"