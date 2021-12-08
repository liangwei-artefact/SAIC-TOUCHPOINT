pt=$3
hive --hivevar pt=$pt -e "
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=2048;
SET hive.exec.max.dynamic.partitions.pernode=1000;
SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb=8192;
INSERT OVERWRITE TABLE marketing_modeling.cdm_ts_wechat_i PARTITION(pt,brand)
SELECT * FROM
(
	SELECT phone AS mobile,
		   cast(regexp_replace(detail['timestamp'], '(\\\\d{4})(\\\\d{2})(\\\\d{2})(\\\\d{2})(\\\\d{2})(\\\\d{2})', '\$1-\$2-\$3 \$4:\$5:\$6') AS TIMESTAMP) AS action_time,
		   CASE
			   WHEN detail['ia_type'] = 'DIG_ACC_SUBSCR' AND detail['brand_id'] = 121 THEN '002001006000_tp'
			   WHEN detail['ia_type'] = 'DIG_ACC_BROADCAST' AND detail['brand_id'] = 121 THEN '002008001001_tp'
			   WHEN detail['ia_type'] = 'DIG_ACC_INBOUND' AND detail['brand_id'] = 121 THEN '002008001002_tp'
			   WHEN detail['ia_type'] = 'DIG_ACC_OUTBOUND' AND detail['brand_id'] = 121 THEN '002008001003_tp'
			   WHEN detail['ia_type'] = 'DIG_ACC_UNSUBSCR' AND detail['brand_id'] = 121 THEN '013001000000_tp'
			   WHEN detail['ia_type'] = 'DIG_ACC_SUBSCR' AND detail['brand_id'] = 101 THEN '002001006000_rw'
			   WHEN detail['ia_type'] = 'DIG_ACC_UNSUBSC' AND detail['brand_id'] = 101 THEN '013001000000_rw'
		   END AS touchpoint_id,
		   pt,
		   CASE
			   WHEN detail['brand_id'] = 121 THEN 'MG'
			   WHEN detail['brand_id'] = 101 THEN 'RW'
			   ELSE NULL
		   END AS brand
	FROM cdp.cdm_cdp_customer_behavior_detail
	WHERE
		TYPE = 'ma_wechat'
		AND pt >= ${pt}
		AND phone regexp '^[1][3-9][0-9]{9}$'
) t1
WHERE
    mobile regexp '^[1][3-9][0-9]{9}$'
    AND action_time IS NOT NULL
    AND touchpoint_id IS NOT NULL
	AND brand IS NOT NULL
"