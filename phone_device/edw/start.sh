#!/bin/bash

job="
use tmp;
set mapreduce.job.queuename=root.ronghui.partner.preonline;
set hive.support.concurrency=false;
set hive.vectorized.execution=ture;

create table if not exists step1_phone (phone_salt string, imei_count int, min_itime int)
partitioned by (data_date string)
row format serde
	'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
stored as orc;

describe tmp.step1_phone;
"
beeline -e "$job" > log_create_tmp_table
