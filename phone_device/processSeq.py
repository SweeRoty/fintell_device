# -*- coding: utf-8 -*-

from __future__ import division
from calendar import monthrange
from ConfigParser import RawConfigParser
from datetime import datetime
from operator import add
import argparse
import time

from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession
from pyspark.sql.window import Window

def getAndroidPairs(spark, data_date):
	global start_time, end_time
	sql = """
		select
			distinct phone_salt,
			imei,
			itime,
			source
		from
			(select
				distinct phone_salt,
				imei,
				itime,
				case when source is null then 'null' else source end source
			from
				ronghui.phone_salt_imei
			where
				data_date <= '{0}'
				and phone_salt is not null
				and imei is not null
				and itime between {1} and {2}
			union all
			select
				distinct phone_salt,
				imei,
				itime,
				case when source is null then 'null' else source end source
			from
				ronghui.jiguang_phone_encrypt
			where
				data_date <= '{0}'
				and phone_salt is not null
				and imei is not null
				and itime between {1} and {2})
	""".format(data_date, start_time, end_time)
	print(sql)
	pairs = spark.sql(sql)
	return pairs

def _generateBias(t):
	global window_size

	phone = t[0]
	records = sorted(t[1], key=lambda row: row['itime'])
	results = []
	total_record_count = 0
	total_devices = set()
	total_sources = set()
	prev_records = []
	for record in records:
		prev_records = [pr for pr in prev_records if record['itime']-pr[2] <= window_size]
		results.append(Row(phone_salt=phone, \
							imei=record['imei'], \
							itime=record['itime'], \
							source=record['source'], \
							total_record_count=total_record_count, \
							total_device_count=len(total_devices), \
							total_source_count=len(total_sources), \
							window_record_count=len(prev_records), \
							window_device_count=len(set([pr[0] for pr in prev_records])), \
							window_source_count=len(set([pr[1] for pr in prev_records])) \
						))
		total_record_count += 1
		total_devices.add(record['imei'])
		total_sources.add(record['source'])
		prev_records.append((record['imei'], record['source'], record['itime']))
	return results

def generateBias(t):
	phone = t[0]
	records = sorted(t[1], key=lambda row: row['itime'])
	results = []
	total_record_count = 0
	total_devices = set()
	total_sources = set()
	prev_records = []
	for record in records:
		row_dict = {}
		row_dict['phone_salt'] = phone
		row_dict['imei'] = record['imei']
		row_dict['itime'] = record['itime']
		row_dict['source'] = record['source']
		tmp_prev_records = prev_records[:]
		for i, window_size in enumerate([365, 180, 90, 30, 7]):
			tmp_prev_records = [pr for pr in tmp_prev_records if record['itime']-pr[2] <= window_size*24*3600]
			row_dict['record_count_in_{}'.format(window_size)] = len(tmp_prev_records)
			row_dict['device_count_in_{}'.format(window_size)] = len(set([pr[0] for pr in tmp_prev_records]))
			row_dict['source_count_in_{}'.format(window_size)] = len(set([pr[1] for pr in tmp_prev_records]))
		row_dict['total_record_count'] = total_record_count
		row_dict['total_device_count'] = len(total_devices)
		row_dict['total_source_count'] = len(total_sources)
		results.append(Row(**row_dict))
		total_record_count += 1
		total_devices.add(record['imei'])
		total_sources.add(record['source'])
		prev_records = [pr for pr in prev_records if record['itime']-pr[2] <= 365*24*3600]
		prev_records.append((record['imei'], record['source'], record['itime']))
	return results

def generateLabels(t):
	phone = t[0]
	records = sorted(t[1], key=lambda row: row['itime'])
	results = []
	i = 0
	while i < len(records)-1:
		j = i+1
		while j < len(records):
			if records[i]['imei'] != records[j]['imei']:
				source = records[i]['source']
				duration = records[j]['itime']-records[i]['itime']
				results.append(Row(source=source, duration=duration, phone_salt=phone, imei=records[i]['imei'], itime=records[i]['itime']))
				break
			else:
				j += 1
		if j == len(records):
			break
		else:
			for k in range(i+1, j):
				source = records[k]['source']
				duration = records[j]['itime']-records[k]['itime']
				results.append(Row(source=source, duration=duration, phone_salt=phone, imei=records[k]['imei'], itime=records[k]['itime']))
		i = j
	return results

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.optionxform = str
	localConf.read('../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_ID_Project___Extract_Sample_or_Bias_from_Sequence') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_month', type=str, help='The format should be YYYYmm')
	parser.add_argument('--kind', type=str, choices=['bias', 'sample'])
	parser.add_argument('--mode', type=str, choices=['train', 'eval', 'test'])
	args = parser.parse_args()
	month_end = str(monthrange(int(args.query_month[:4]), int(args.query_month[4:6]))[1])
	data_date = args.query_month+month_end
	start_time = time.mktime(datetime.strptime('2016-01-01', '%Y-%m-%d').timetuple())
	begin_time = time.mktime(datetime.strptime('2017-01-01', '%Y-%m-%d').timetuple())
	end_date = datetime.strptime('{} 23:59:59'.format(data_date), '%Y%m%d %H:%M:%S')
	end_time = time.mktime(end_date.timetuple())
	end_date = end_date.strftime('%Y-%m-%d')

	print('====> Start computation')
	pairs = getAndroidPairs(spark, data_date)
	if args.kind == 'bias':
		phones = spark.read.csv('/user/ronghui_safe/hgy/nid/samples/{}_{}'.format(args.query_month, args.mode), header=True).select('phone_salt').distinct()
		pairs = pairs.join(phones, on='phone_salt', how='inner')
		features = pairs.rdd.map(lambda row: (row['phone_salt'], row)).groupByKey().flatMap(generateBias).toDF()
		features.repartition(50).write.csv('/user/ronghui_safe/hgy/nid/features/bias_{}_{}'.format(args.query_month, args.mode), header=True)
	else:
		phones = spark.read.csv('/user/ronghui_safe/hgy/nid/active_phone_imei_count_{}'.format(args.query_month), header=True)
		if args.mode != 'test':
			phones = phones.where(F.col('imei_count').between(2, 120))
			phones = phones.select(F.col('phone').alias('phone_salt'))#.sample(False, 0.05, 11267)
			pairs = pairs.join(phones, on='phone_salt', how='inner')
		else:
			phones_1 = phones.where(F.col('imei_count').between(1, 120))
			phones_1 = phones_1.select(F.col('phone').alias('phone_salt'))
			pairs_1 = pairs.join(phones_1, on='phone_salt', how='inner')
			phones_2 = phones.where(F.col('imei_count') > 120)
			phones_2 = phones_2.select(F.col('phone').alias('phone_salt'))
			pairs_2 = pairs.join(phones_2, on='phone_salt', how='inner')
			pairs_2 = pairs_2.withColumn('key', F.concat_ws('_', F.col('phone_salt'), F.col('imei')))
			pairs_2 = pairs_2.withColumn('row_num', F.row_number().over(Window.partitionBy('key').orderBy(F.col('itime').desc())))
			pairs_2 = pairs_2.where(F.col('row_num') <= 120).select(pairs_1.columns)
			pairs = pairs_1.union(pairs_2)
		samples = None
		if args.mode != 'test':
			samples = pairs.rdd.map(lambda row: (row['phone_salt'], row)).groupByKey().flatMap(generateLabels).toDF()
			phone_min_itime = pairs.rdd \
									.map(lambda row: (row['phone_salt'], row['itime'])) \
									.reduceByKey(lambda x, y: min(x, y)) \
									.map(lambda t: Row(phone_salt=t[0], min_itime=t[1])) \
									.toDF() \
									.where(F.col('min_itime') >= begin_time)
			samples = samples.join(phone_min_itime, on='phone_salt', how='inner')
		else:
			samples = pairs.withColumn('duration', F.lit(-1))
			samples = samples.select(['source', 'duration', 'phone_salt', 'imei', 'itime'])
		samples.repartition(50).write.csv('/user/ronghui_safe/hgy/nid/samples/{}_{}'.format(args.query_month, args.mode), header=True)