pt=$3
hive --hivevar pt=$pt -e "
set hive.exec.dynamic.partition.mode=nonstrict;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
SET hive.exec.max.dynamic.partitions=2048;
SET hive.exec.max.dynamic.partitions.pernode=1000;

insert overwrite table marketing_modeling.dw_ts_ccm_activity_i PARTITION (pt,brand)
select * from
(
	select
		cellphone as mobile,
		created_date as action_time,
		case
			when action = 'INCREASE' AND brand = 'MG' AND description like '%签到%' then '002009001003_tp' -- 签到获取积分
			when action = 'INCREASE' AND description like '%完善个人资料%' AND brand = 'MG' then '002009001004_tp' -- 完善个人资料获取积分
			when action = 'INCREASE' AND brand = 'MG' then '002009001001_tp' -- 其他获取积分（其他社区行为	社区积分变动）
			when action = 'DECREASE' AND brand = 'MG' then '002009001002_tp' -- 消除积分（其他社区行为	社区积分变动）

			when action = 'INCREASE' AND description like '%签到%' AND brand = 'RW' then '002002015002_rw' -- 签到获取积分
			when action = 'INCREASE' AND description like '%完善个人资料%' AND brand = 'RW' then '002002015003_rw' -- 完善个人资料获取积分
			when action = 'INCREASE' AND brand = 'RW' then '002002015001_rw' -- 其他获取积分（其他社区行为	社区积分变动）
			when action = 'DECREASE' AND brand = 'RW' then '002002015004_rw' -- 消除积分（其他社区行为	社区积分变动）
			else NULL
		end as touchpoint_id,
		description,
		pt,
		brand
	from
	(
		select 
			uid, 
			created_date, 
			action,
			description,
			case
				when brand_code = 2 then 'MG'
				when brand_code = 1 then 'RW'
			else NULL end as brand,
			regexp_replace(to_date(created_date), '-', '') as pt
		from dtwarehouse.ods_ccmpoint_points_record -- 全量表
		where 
			pt = '${pt}'  -- 这行是取所有pt数据
			AND regexp_replace(to_date(created_date), '-', '') >= '${pt}'
			AND status = 'SUCCESS'
			AND action in ('INCREASE', 'DECREASE')
			AND description not like '%注册%' -- 排除注册行为
			AND description not in ('元宵集卡成功','初一集卡成功','初三集卡成功','初五集卡成功','初夕集卡成功',
			'活动分享成功','除夕集卡成功','集卡完成','OOTD系列活动获赞前20','新年徽章设计1等奖','新年徽章设计2等奖',
			'新年徽章设计3等奖','签到1天','签到3天','名爵积分抽奖赠送积分','推荐有礼','论坛四重礼一起瓜分200万积分奖励',
			'上汽MG全国电竞邀请赛')
			AND description not like '%活动%'
	) a
	join 
	(
		select 
			cellphone, uid
		from 
		(
			select 
				cellphone, 
				uid, 
				Row_Number() OVER (partition by uid ORDER BY regist_date) rank_num 
			from dtwarehouse.ods_ccm_member
			where 
				pt = '${pt}' -- 生产环境下用这一行
				AND cellphone IS NOT NULL
				AND uid IS NOT NULL
		) b0
		where rank_num = 1 
	) b
	on a.uid = b.uid
) t1
where
	mobile regexp '^[1][3-9][0-9]{9}$'
	AND action_time IS NOT NULL
	AND touchpoint_id IS NOT NULL
	AND brand IS NOT NULL
"
