#!/bin/bash

hadoop fs -mkdir -p /user/hive/warehouse/ronghui_bj.db/etl_v1_2/log/nid/

query_month=$1

spark-submit extractPhones.py --query_month $query_month > log_step1_$query_month
hadoop fs -put log_step1_$query_month /user/hive/warehouse/ronghui_bj.db/etl_v1_2/log/nid/

spark-submit extractSamples.py --query_month $query_month > log_step2_$query_month
hadoop fs -put log_step2_$query_month /user/hive/warehouse/ronghui_bj.db/etl_v1_2/log/nid/

spark-submit extractFeatures.py --query_month $query_month > log_step3_$query_month
hadoop fs -put log_step3_$query_month /user/hive/warehouse/ronghui_bj.db/etl_v1_2/log/nid/

spark-submit preprocess.py --query_month $query_month > log_step4_$query_month
hadoop fs -put log_step4_$query_month /user/hive/warehouse/ronghui_bj.db/etl_v1_2/log/nid/

/opt/spark3/bin/spark-submit glm.py --query_month $query_month > log_step5_$query_month
hadoop fs -put log_step5_$query_month /user/hive/warehouse/ronghui_bj.db/etl_v1_2/log/nid/

spark-submit build_bipartite.py --query_month $query_month > log_step6_$query_month
hadoop fs -put log_step6_$query_month /user/hive/warehouse/ronghui_bj.db/etl_v1_2/log/nid/

spark-submit bipartite_lpa.py --query_month $query_month > log_step7_$query_month
hadoop fs -put log_step7_$query_month /user/hive/warehouse/ronghui_bj.db/etl_v1_2/log/nid/