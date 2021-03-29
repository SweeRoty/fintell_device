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

create table if not exists step2_sample (phone_salt string, imei string, itime int, source string, min_itime int)
partitioned by (data_date string)
row format serde
	'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
stored as orc;
describe tmp.step2_sample;

create table if not exists step3_feature (
	record_count_in_365 int, 
	device_count_in_365 int,
	source_count_in_365 int,
	record_count_in_180 int, 
	device_count_in_180 int,
	source_count_in_180 int,
	record_count_in_90 int, 
	device_count_in_90 int,
	source_count_in_90 int,
	record_count_in_30 int, 
	device_count_in_30 int,
	source_count_in_30 int,
	record_count_in_7 int, 
	device_count_in_7 int,
	source_count_in_7 int)
partitioned by (data_date string)
row format serde
	'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
stored as orc;
describe tmp.step3_feature;
"
beeline -e "$job" > log_create_tmp_table