pt=$3
hive --hivevar pt=$pt -e "
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=2048;
SET hive.exec.max.dynamic.partitions.pernode=1000;
SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb=8192;
INSERT OVERWRITE TABLE marketing_modeling.cdm_ts_followup_i PARTITION (pt,brand)
SELECT * FROM
(SELECT phone AS mobile,
       detail['actual_follow_time'] AS action_time,
       CASE
           WHEN detail['follow_way'] = '邀约' and detail['brand_id'] = 121 THEN '009002001000_tp'
           WHEN detail['follow_way'] = '电话' and detail['brand_id'] = 121 THEN '009002002000_tp'
           WHEN detail['follow_way'] = '展厅接待' and detail['brand_id'] = 121 THEN '009002003000_tp'
           WHEN detail['follow_way'] = '进入展厅' and detail['brand_id'] = 121 THEN '009002004000_tp'
           WHEN detail['follow_way'] = '短信' and detail['brand_id'] = 121 THEN '009002005000_tp'
           WHEN detail['follow_way'] = '参加外展' and detail['brand_id'] = 121 THEN '009002006000_tp'
           WHEN detail['follow_way'] = '微信' and detail['brand_id'] = 121 THEN '009002008000_tp'
           WHEN detail['follow_way'] = '客户来电' and detail['brand_id'] = 121 THEN '009002009000_tp'
           WHEN detail['follow_way'] = '上门拜访' and detail['brand_id'] = 121 THEN '009002010000_tp'
           WHEN detail['follow_way'] = '市场活动' and detail['brand_id'] = 121 THEN '009002011000_tp'
           WHEN detail['follow_way'] = '电子邮件' and detail['brand_id'] = 121 THEN '009002012000_tp'
           WHEN detail['brand_id'] = 121 THEN '009002007000_tp'

           WHEN detail['follow_way'] = '邀约' and detail['brand_id'] = 101 THEN '009002001000_rw'
           WHEN detail['follow_way'] = '电话' and detail['brand_id'] = 101 THEN '009002002000_rw'
           WHEN detail['follow_way'] = '展厅接待' and detail['brand_id'] = 101 THEN '009002003000_rw'
           WHEN detail['follow_way'] = '进入展厅' and detail['brand_id'] = 101 THEN '009002004000_rw'
           WHEN detail['follow_way'] = '短信'  and detail['brand_id'] = 101 THEN '009002005000_rw'
           WHEN detail['follow_way'] = '参加外展' and detail['brand_id'] = 101 THEN '009002006000_rw'
           WHEN detail['follow_way'] = '微信' and detail['brand_id'] = 101 THEN '009002008000_rw'
           WHEN detail['follow_way'] = '客户来电' and detail['brand_id'] = 101 THEN '009002009000_rw'
           WHEN detail['follow_way'] = '上门拜访' and detail['brand_id'] = 101 THEN '009002010000_rw'
           WHEN detail['follow_way'] = '市场活动' and detail['brand_id'] = 101 THEN '009002011000_rw'
           WHEN detail['follow_way'] = '电子邮件' and detail['brand_id'] = 101 THEN '009002012000_rw'
           WHEN detail['brand_id'] = 101 THEN '009002007000_rw'
       END AS touchpoint_id,
       pt,
       CASE
           WHEN detail['brand_id'] = 121 THEN 'MG'
           WHEN detail['brand_id'] = 101 THEN 'RW'
       END AS brand
FROM cdp.cdm_cdp_customer_behavior_detail
WHERE TYPE = 'follow_up'
AND pt >= ${pt}
) t1
WHERE
    mobile regexp '^[1][3-9][0-9]{9}$'
	AND action_time IS NOT NULL
	AND touchpoint_id IS NOT NULL
	AND brand IS NOT NULL
"