pt=$3
hive --hivevar pt=$pt -e "
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamici.partition=true;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
SET hive.exec.max.dynamic.partitions=2048;
SET hive.exec.max.dynamic.partitions.pernode=1000;
set hive.execution.engine=mr;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join = false;

INSERT OVERWRITE TABLE marketing_modeling.cdm_ts_community_i PARTITION (pt,brand)
SELECT * FROM
(		SELECT
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
		pt,
		CASE
			WHEN detail['brand_id'] = '121' THEN 'MG'
			WHEN detail['brand_id'] = '101' THEN 'RW'
		ELSE NULL END AS brand
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
		pt,
		brand
	FROM
(
  SELECT
			detail['ts'] AS action_time,
			CASE
				when detail['duration'] >= 0 AND detail['duration'] < 5 then '002002002001_tp' -- 社区文章浏览[0,5)s
				when detail['duration'] >= 5 AND detail['duration'] < 30 then '002002002002_tp' -- 社区文章浏览[5,30)s
				when detail['duration'] >= 30 then '002002002003_tp' -- 社区文章浏览>=30s
			END AS touchpoint_id,
			'MG' AS brand,
			pt
		FROM
		    cdp.cdm_cdp_customer_behavior_detail
    where pt >= '${pt}' and type='contactor'
			AND detail['pagetype'] = '资讯详情页'
			AND detail['applicationname'] = 'MGAPP'
 ) p1
	UNION ALL

	SELECT
		cellphone AS mobile, action_time, touchpoint_id, pt, brand
	FROM
	(
		SELECT
			user_id,
			publish_date AS action_time,
			CASE
				WHEN brand_type = 2 THEN '002003001001_tp'
				WHEN brand_type = 1 THEN '002002006001_rw'
			ELSE NULL END AS touchpoint_id, -- 发布文章
			REGEXP_REPLACE(TO_DATE(publish_date), '-', '') AS pt,
			CASE
				WHEN brand_type = 2 then 'MG'
				WHEN brand_type = 1 then 'RW'
			ELSE NULL END AS brand
		FROM dtwarehouse.ods_bbscomm_tt_community_article
		WHERE
		pt = '${pt}' -- 取所有数据
		AND REGEXP_REPLACE(TO_DATE(publish_date), '-', '') >= '${pt}'

		UNION ALL

		SELECT
			saic_user_id AS user_id,
			publish_time AS action_time,
			CASE
				WHEN brand_type = 2 THEN '002003001001_tp'
				WHEN brand_type = 1 THEN '002002006001_rw'
			ELSE NULL END AS touchpoint_id, -- 发布文章
			REGEXP_REPLACE(TO_DATE(publish_time), '-', '') AS pt,
			CASE
				WHEN brand_type = 2 then 'MG'
				WHEN brand_type = 1 then 'RW'
			ELSE NULL END AS brand
		FROM dtwarehouse.ods_bbscomm_bbs_content
		WHERE
		pt = '${pt}'
		AND REGEXP_REPLACE(TO_DATE(publish_time), '-', '') >= '${pt}'

		UNION ALL

		SELECT
			user_id,
			create_date AS action_time,
			CASE WHEN brand_type = 2 THEN '002003001002_tp'
			WHEN brand_type = 1 THEN '002002006002_rw'
			ELSE NULL END AS touchpoint_id, -- 发布新闻
			REGEXP_REPLACE(TO_DATE(create_date), '-', '') AS pt,
			CASE
				WHEN brand_type = 2 then 'MG'
				WHEN brand_type = 1 then 'RW'
			ELSE NULL END AS brand
		FROM dtwarehouse.ods_bbscomm_tt_community_news
		WHERE
		pt = '${pt}'
		AND REGEXP_REPLACE(TO_DATE(create_date), '-', '') >= '${pt}'

		UNION ALL

		SELECT
			publisher_id AS user_id,
			create_time AS action_time,
			CASE
				WHEN brand_type = 2 THEN '002003001003_tp'
				WHEN brand_type = 1 THEN '002002006003_rw'
			ELSE NULL END AS touchpoint_id, -- 发布活动
			REGEXP_REPLACE(TO_DATE(create_time), '-', '') AS pt,
			CASE
				WHEN brand_type = 2 then 'MG'
				WHEN brand_type = 1 then 'RW'
			ELSE NULL END AS brand
		FROM dtwarehouse.ods_bbscomm_tt_saic_activity
	  WHERE
	    pt = '${pt}'
		  AND REGEXP_REPLACE(TO_DATE(create_time), '-', '') >= '${pt}'
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
			  pt = '${pt}'
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