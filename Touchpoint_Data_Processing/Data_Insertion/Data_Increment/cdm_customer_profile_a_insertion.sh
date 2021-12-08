pt=$3
queue_name=marketing_modeling

spark-submit --master yarn  \
--driver-memory 5G  \
--num-executors 10 \
--executor-cores 10 \
--executor-memory 32G \
--conf "spark.excutor.memoryOverhead=10G"  \
--queue $queue_name \
cdm_customer_profile_processing.py $pt
