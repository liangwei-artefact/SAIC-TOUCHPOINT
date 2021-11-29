pt1=$3
pt2=$4
hive --hivevar pt1=$pt1 --hivevar pt2=$pt2 -e "
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=2048;
SET hive.exec.max.dynamic.partitions.pernode=1000;
SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb=8192;
INSERT OVERWRITE TABLE marketing_modeling.dw_ts_scrm_i PARTITION(pt,brand)
SELECT * FROM
(
	SELECT 
		t.mobile AS mobile,
		t.starttime AS action_time,
		CASE
           WHEN t.TYPE = 'view' AND t.module = 'custom' AND staff_brand = 'M' THEN '009004001001_tp' --浏览客户分享上报
           WHEN t.TYPE = 'view' AND t.module = 'h5' AND staff_brand = 'M' THEN '009004001002_tp' --浏览外链
           WHEN t.TYPE = 'view' AND t.module = 'image'AND staff_brand = 'M' THEN '009004001003_tp' --浏览图片
           WHEN t.TYPE = 'view' AND t.module = 'news' AND staff_brand = 'M' THEN '009004001004_tp' --浏览资讯
           WHEN t.TYPE = 'view' AND t.module = 'show' AND staff_brand = 'M' THEN '009004001005_tp' -- 浏览H5文案
           WHEN t.TYPE = 'view' AND t.module = 'video' AND staff_brand = 'M' THEN '009004001006_tp' -- 浏览视频
           WHEN t.TYPE = 'share' AND t.module = 'custom' AND staff_brand = 'M' THEN '009004002001_tp' -- 分享客户分享上报
           WHEN t.TYPE = 'share' AND t.module = 'h5' AND staff_brand = 'M' THEN '009004002002_tp' -- 分享外链
           WHEN t.TYPE = 'share' AND t.module = 'image' AND staff_brand = 'M' THEN '009004002003_tp' -- 分享图片
           WHEN t.TYPE = 'share' AND t.module = 'news' AND staff_brand = 'M' THEN '009004002004_tp' -- 分享资讯
           WHEN t.TYPE = 'share' AND t.module = 'show' AND staff_brand = 'M' THEN '009004002005_tp' -- 分享H5文案
           WHEN t.TYPE = 'share' AND t.module = 'video' AND staff_brand = 'M' THEN '009004002006_tp' -- 分享视频

           WHEN t.TYPE = 'view' AND t.module = 'custom' AND staff_brand = 'R' THEN '009004001001_rw' --浏览客户分享上报
           WHEN t.TYPE = 'view' AND t.module = 'h5' AND staff_brand = 'R' THEN '009004001002_rw' --浏览外链
           WHEN t.TYPE = 'view' AND t.module = 'image' AND staff_brand = 'R' THEN '009004001003_rw' --浏览图片
           WHEN t.TYPE = 'view' AND t.module = 'news' AND staff_brand = 'R' THEN '009004001004_rw' --浏览资讯
           WHEN t.TYPE = 'view' AND t.module = 'show' AND staff_brand = 'R' THEN '009004001005_rw' -- 浏览H5文案
           WHEN t.TYPE = 'view' AND t.module = 'video' AND staff_brand = 'R' THEN '009004001006_rw' -- 浏览视频
           WHEN t.TYPE = 'share' AND t.module = 'custom' AND staff_brand = 'R' THEN '009004002001_rw' -- 分享客户分享上报
           WHEN t.TYPE = 'share' AND t.module = 'h5'  AND staff_brand = 'R' THEN '009004002002_rw' -- 分享外链
           WHEN t.TYPE = 'share' AND t.module = 'image' AND staff_brand = 'R' THEN '009004002003_rw' -- 分享图片
           WHEN t.TYPE = 'share' AND t.module = 'news' AND staff_brand = 'R' THEN '009004002004_rw' -- 分享资讯
           WHEN t.TYPE = 'share' AND t.module = 'show' AND staff_brand = 'R' THEN '009004002005_rw' -- 分享H5文案
           WHEN t.TYPE = 'share' AND t.module = 'video' AND staff_brand = 'R' THEN '009004002006_rw' -- 分享视频
		END AS touchpoint_id,
		CASE
			WHEN staff_brand = 'M' THEN 'MG'
			WHEN staff_brand = 'R' THEN 'RW'
			ELSE NULL
			END AS brand,
			date_format(t.starttime,'yyyyMMdd') AS pt
	FROM
	(
		SELECT 
		mobile,TYPE,module,starttime,staff_brand
		FROM
		(
			SELECT 
				TYPE,module,starttime,openid,staff_brand
			FROM dtwarehouse.ods_scrm_saic_statistics_interval
			WHERE 
				pt = ${pt2}
				AND regexp_replace(to_date(starttime), '-', '') >= ${pt1}
				AND regexp_replace(to_date(starttime), '-', '') <= ${pt2}
		  ) a
		LEFT JOIN(SELECT mobile,openid FROM linkflow.ods_scrmtools_saic_user WHERE pt = ${pt2}) b 
		ON a.openid = b.openid
		WHERE 
			mobile regexp '^[1][3-9][0-9]{9}$'
			AND TYPE IS NOT NULL
			AND module IS NOT NULL
	) t

	UNION ALL

	SELECT
		check_in_mobile AS mobile,
		apply_date AS action_time,
		CASE
			 WHEN brand_id = 121 THEN '008001010002_tp' -- SCRM活动签到
			 WHEN brand_id = 101 THEN '008001010002_rw'
			 END AS touchpoint_id,
		CASE
			 WHEN brand_id = 121 THEN 'MG'
			 WHEN brand_id = 101 THEN 'RW'
			 ELSE NULL
		END AS brand,
		date_format(apply_date,'yyyyMMdd') AS pt
	FROM 
	(SELECT * FROM dtwarehouse.ods_scrm_saic_activity_apply_cust
		WHERE
			pt = ${pt2}
			AND regexp_replace(to_date(apply_date), '-', '') >= ${pt1}
			AND regexp_replace(to_date(apply_date), '-', '') <= ${pt2}
	)a
	LEFT JOIN
	(
		SELECT 
			id, brand_id
		FROM (SELECT id, dealer_id FROM dtwarehouse.ods_dlm_t_cust_base WHERE pt = ${pt2}) a
		LEFT JOIN (SELECT dlm_org_id, brand_id FROM dtwarehouse.ods_rdp_v_sales_region_dealer WHERE pt = ${pt2}) b
		ON a.dealer_id = b.dlm_org_id
	) b 
	ON a.cust_id = b.id

	UNION ALL

	SELECT 
		mobile,
		add_time AS action_time,
		CASE
			 WHEN brand_id = 121 THEN '009001000000_tp' -- 添加销售代表企业微信
			 WHEN brand_id = 101 THEN '009001000000_rw'
			 END AS touchpoint_id,
		CASE
			WHEN brand_id = 121 THEN 'MG'
			WHEN brand_id = 101 THEN 'RW'
			ELSE NULL
		END AS brand,
		date_format(add_time,'yyyyMMdd') AS pt
	FROM 
	(
		SELECT 
			mobile,dealer_id,add_time,pt
		 FROM dtwarehouse.ods_scrm_crm_customer_add
		 WHERE 
			pt = ${pt2}
			AND mobile regexp '^[1][3-9][0-9]{9}$'
			AND regexp_replace(to_date(add_time), '-', '') >= ${pt1}
			AND regexp_replace(to_date(add_time), '-', '') <= ${pt2}
	) a
	LEFT JOIN
	(SELECT dlm_org_id,brand_id FROM dtwarehouse.ods_rdp_v_sales_region_dealer WHERE pt = ${pt2}) b
	ON a.dealer_id = b.dlm_org_id
) t1
WHERE
    mobile regexp '^[1][3-9][0-9]{9}$'
    AND action_time IS NOT NULL
    AND touchpoint_id IS NOT NULL
	AND brand IS NOT NULL
"