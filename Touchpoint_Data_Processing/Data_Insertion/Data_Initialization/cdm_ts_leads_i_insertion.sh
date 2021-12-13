pt1=$3
pt2=$4
queue_name=marketing_modeling

spark-submit --master yarn  \
--driver-memory 5G  \
--num-executors 10 \
--executor-cores 10 \
--executor-memory 32G \
--conf "spark.excutor.memoryOverhead=10G"  \
--queue $queuename \
cdm_ts_leads_processing.py $pt1 $pt2
