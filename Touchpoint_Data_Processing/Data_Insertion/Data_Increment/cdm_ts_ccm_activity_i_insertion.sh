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

insert overwrite table marketing_modeling.cdm_ts_ccm_activity_i PARTITION (pt,brand)
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
      phone cellphone,
      to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss') created_date,
      detail['description'] description,
      case
    		when detail['brand_code'] = 2 then 'MG'
    		when detail['brand_code'] = 1 then 'RW'
    	else NULL end as brand,
      detail['action'] action,
      regexp_replace(to_date( to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss')), '-', '') as pt
      from cdp.cdm_cdp_customer_behavior_detail
      where type ='score'
      and   pt = ${pt}
			AND regexp_replace(to_date(to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss')), '-', '') >= '${pt}'
			AND  detail['action'] in ('INCREASE', 'DECREASE')
			AND detail['description'] not like '%注册%' -- 排除注册行为
			AND detail['description'] not in ('元宵集卡成功','初一集卡成功','初三集卡成功','初五集卡成功','初夕集卡成功',
			'活动分享成功','除夕集卡成功','集卡完成','OOTD系列活动获赞前20','新年徽章设计1等奖','新年徽章设计2等奖',
			'新年徽章设计3等奖','签到1天','签到3天','名爵积分抽奖赠送积分','推荐有礼','论坛四重礼一起瓜分200万积分奖励',
			'上汽MG全国电竞邀请赛')
			AND detail['description'] not like '%活动%'
		and phone regexp '^[1][3-9][0-9]{9}$'
			AND to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss') IS NOT NULL
	) a
) t1
where
 touchpoint_id IS NOT NULL
	AND brand IS NOT NULL
"
