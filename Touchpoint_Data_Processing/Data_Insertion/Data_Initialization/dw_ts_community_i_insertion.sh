pt1=$3
pt2=$4
hive --hivevar pt1=$pt1 --hivevar pt2=$pt2 -e "
set hive.exec.dynamic.partition.mode=nonstrict;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
SET hive.exec.max.dynamic.partitions=2048;
SET hive.exec.max.dynamic.partitions.pernode=1000;

INSERT OVERWRITE TABLE marketing_modeling.dw_ts_community_i PARTITION (pt,brand)
SELECT * FROM
(
		SELECT 
		phone AS mobile,
		cast(detail['behavior_time'] AS timestamp) AS action_time,
		CASE
			when detail['action_type'] = '浏览' AND detail['brand_id'] = '101' then '002002003001_rw'
			when detail['action_type'] = '评论' AND detail['brand_id'] = '121' then '002004001000_tp'
			when detail['action_type'] = '点赞' AND detail['brand_id'] = '121' then '002005001000_tp' 
			when detail['action_type'] = '转发' AND detail['brand_id'] = '121' then '002006001000_tp'
			when detail['action_type'] = '收藏' AND detail['brand_id'] = '121' then '002007001000_tp' 
			else NULL
		END AS touchpoint_id,
		CASE 
			WHEN detail['brand_id'] = '121' THEN 'MG' 
			WHEN detail['brand_id'] = '101' THEN 'RW' 
		ELSE NULL END AS brand,
		pt
	FROM cdp.cdm_cdp_customer_behavior_detail
	WHERE 
		pt >= '${pt1}' AND pt <= '${pt2}'
		AND type = 'informations' 
		AND phone IS NOT NULL
		AND detail['action_type'] in ('浏览', '评论', '点赞', '转发', '收藏')

	UNION ALL

	SELECT 
		phone AS mobile, 
		action_time,
		touchpoint_id,
		brand,
		pt
	FROM
	(
		SELECT 
			loginuserid, 
			ts AS action_time,
			CASE 
				when duration >= 0 AND duration < 5 then '002002002001_tp' -- 社区文章浏览[0,5)s
				when duration >= 5 AND duration < 30 then '002002002002_tp' -- 社区文章浏览[5,30)s
				when duration >= 30 then '002002002003_tp' -- 社区文章浏览>=30s
			END AS touchpoint_id,
			'MG' AS brand,
			pt
		FROM dtwarehouse.cdm_growingio_activity_hma
		WHERE 
			pt >= '${pt1}' AND pt <= '${pt2}'
			AND pagevariable['pagetype_pvar'] = '资讯详情页'
			AND applicationname = 'MGAPP'
	) a
	JOIN
	(
		SELECT 
			cellphone AS phone, uid
		FROM 
		(
			SELECT 
				cellphone, uid, 
				Row_Number() OVER (partition by uid ORDER BY regist_date) rank_num 
			FROM dtwarehouse.ods_ccm_member
			WHERE pt = '${pt2}'
		) b0
		WHERE rank_num = 1 
	) b
	ON a.loginuserid = b.uid

	UNION ALL

	SELECT 
		cellphone AS mobile, action_time, touchpoint_id, brand, pt
	FROM 
	(
		SELECT 
			user_id, 
			publish_date AS action_time, 
			CASE 
				WHEN brand_type = 2 THEN '002003001001_tp'
				WHEN brand_type = 1 THEN '002002006001_rw'
			ELSE NULL END AS touchpoint_id, -- 发布文章
			CASE 
				WHEN brand_type = 2 then 'MG'
				WHEN brand_type = 1 then 'RW'
			ELSE NULL END AS brand,
			REGEXP_REPLACE(TO_DATE(publish_date), '-', '') AS pt
		FROM dtwarehouse.ods_bbscomm_tt_community_article 
		WHERE 
			pt = '${pt2}'
			AND REGEXP_REPLACE(TO_DATE(publish_date), '-', '') >= '${pt1}'
			AND REGEXP_REPLACE(TO_DATE(publish_date), '-', '') <= '${pt2}'
			
		UNION ALL

		SELECT 
			saic_user_id AS user_id,
			publish_time AS action_time, 
			CASE 
				WHEN brand_type = 2 THEN '002003001001_tp'
				WHEN brand_type = 1 THEN '002002006001_rw'
			ELSE NULL END AS touchpoint_id, -- 发布文章
			CASE 
				WHEN brand_type = 2 then 'MG'
				WHEN brand_type = 1 then 'RW'
			ELSE NULL END AS brand,
			REGEXP_REPLACE(TO_DATE(publish_time), '-', '') AS pt
		FROM dtwarehouse.ods_bbscomm_bbs_content
		WHERE 
			pt = '${pt2}'
			AND REGEXP_REPLACE(TO_DATE(publish_time), '-', '') >= '${pt1}'
			AND REGEXP_REPLACE(TO_DATE(publish_time), '-', '') <= '${pt2}'

		UNION ALL

		SELECT 
			user_id, 
			create_date AS action_time, 
			CASE WHEN brand_type = 2 THEN '002003001002_tp'
			WHEN brand_type = 1 THEN '002002006002_rw'
			ELSE NULL END AS touchpoint_id, -- 发布新闻
			CASE 
				WHEN brand_type = 2 then 'MG'
				WHEN brand_type = 1 then 'RW'
			ELSE NULL END AS brand,
			REGEXP_REPLACE(TO_DATE(create_date), '-', '') AS pt
		FROM dtwarehouse.ods_bbscomm_tt_community_news 
		WHERE 
			pt = '${pt2}'
			AND REGEXP_REPLACE(TO_DATE(create_date), '-', '') >= '${pt1}'
			AND REGEXP_REPLACE(TO_DATE(create_date), '-', '') <= '${pt2}'

		UNION ALL

		SELECT 
			publisher_id AS user_id, 
			create_time AS action_time,
			CASE 
				WHEN brand_type = 2 THEN '002003001003_tp'
				WHEN brand_type = 1 THEN '002002006003_rw'
			ELSE NULL END AS touchpoint_id, -- 发布活动
			CASE 
				WHEN brand_type = 2 then 'MG'
				WHEN brand_type = 1 then 'RW'
			ELSE NULL END AS brand,
			REGEXP_REPLACE(TO_DATE(create_time), '-', '') AS pt
		FROM dtwarehouse.ods_bbscomm_tt_saic_activity  
		WHERE 
			pt = '${pt2}'
			AND REGEXP_REPLACE(TO_DATE(create_time), '-', '') >= '${pt1}'
			AND REGEXP_REPLACE(TO_DATE(create_time), '-', '') <= '${pt2}'
	) a 
	JOIN 
	(
		SELECT 
			cellphone, uid
		FROM 
		(
			SELECT 
				cellphone, uid, 
				ROW_NUMBER() OVER (PARTITION BY uid ORDER BY regist_date) rank_num 
			FROM dtwarehouse.ods_ccm_member
			WHERE 
				pt = '${pt2}'
				AND cellphone IS NOT NULL
				AND uid IS NOT NULL
		) b0
		WHERE rank_num = 1 
	) b
	ON a.user_id = b.uid
) t1
WHERE 
	mobile regexp '^[1][3-9][0-9]{9}$'
	AND action_time IS NOT NULL
	AND touchpoint_id IS NOT NULL
	AND brand IS NOT NULL
"