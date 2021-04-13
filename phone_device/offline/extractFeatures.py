# -*- coding: utf-8 -*-

from __future__ import division
from calendar import monthrange
from ConfigParser import RawConfigParser
from datetime import datetime
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

def generateBias(t):
	global max_window_size
	phone = t[0]
	records = sorted(t[1], key=lambda row: row['itime'])
	results = []
	"""
	total_record_count = 0
	total_devices = set()
	total_sources = set()
	"""
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
		"""
		row_dict['total_record_count'] = total_record_count
		row_dict['total_device_count'] = len(total_devices)
		row_dict['total_source_count'] = len(total_sources)
		"""
		results.append(Row(**row_dict))
		"""
		total_record_count += 1
		total_devices.add(record['imei'])
		total_sources.add(record['source'])
		"""
		prev_records = [pr for pr in prev_records if record['itime']-pr[2] <= max_window_size]
		prev_records.append((record['imei'], record['source'], record['itime']))
	return results

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.optionxform = str
	localConf.read('../../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_ID_Project___Extract_Bias_from_Sequence') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_month', type=str, help='The format should be YYYYmm')
	parser.add_argument('--mode', type=str, choices=['train', 'eval', 'test'])
	args = parser.parse_args()

	fr_start_time = time.mktime(datetime.strptime('{}01'.format(args.query_month), '%Y%m%d').timetuple())
	month_end = str(monthrange(int(args.query_month[:4]), int(args.query_month[4:6]))[1])
	data_date = args.query_month + month_end
	end_time = time.mktime(datetime.strptime('{} 23:59:59'.format(data_date), '%Y%m%d %H:%M:%S').timetuple())
	max_window_size = 365*24*3600
	start_time = time.mktime(datetime.strptime('20160101', '%Y%m%d').timetuple())

	print('====> Start computation')
	pairs = getAndroidPairs(spark, data_date)
	phones = spark.read.csv('/user/ronghui_safe/hgy/nid/samples/{}_{}'.format(args.query_month, args.mode), header=True, inferSchema=True).select('phone_salt').distinct()
	pairs = pairs.join(phones, on='phone_salt', how='inner').repartition(40000)
	features = pairs.rdd.map(lambda row: (row['phone_salt'], row)).groupByKey(40000)
	phone_stats = features.map(lambda t: Row(phone_salt=t[0], record_count=len(t[1]))).toDF()
	phone_stats = phone_stats.where(F.col('record_count') < 2000).rdd.map(lambda row: (row['phone_salt'], None))
	features = features.join(phone_stats).map(lambda t: (t[0], t[1][0])).flatMap(generateBias).toDF()
	if args.mode == 'test':
		features = features.where(F.col('itime') >= fr_start_time)
	features.repartition(50).write.csv('/user/ronghui_safe/hgy/nid/features/bias_{}_{}'.format(args.query_month, args.mode), header=True)