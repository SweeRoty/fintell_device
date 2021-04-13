# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
from datetime import datetime
from operator import add
import argparse
import time

from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession

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
				source
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
				source
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

def getActiveAndroids(spark, data_date):
	sql = """
		select
			imei
		from
			ronghui_mart.rh_online_imei_3m_android
		where
			data_date = '{0}'
	""".format(data_date)
	print(sql)
	devices = spark.sql(sql)
	return devices

def getAbnormalAndroids(spark, data_date):
	sql = """
		select
			imei,
			shanzhai_flag flag
		from
			ronghui_mart.sz_device_list
		where
			data_date = '{0}'
	""".format(data_date)
	print(sql)
	devices = spark.sql(sql)
	return devices

def transform2row(row_dict):
	return Row(**row_dict)

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.optionxform = str
	localConf.read('../../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_ID_Project___Extract_Valid_Phones') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_month', type=str, help='The format should be YYYYmm')
	parser.add_argument('--print_source_stats', action='store_true', default=False)
	parser.add_argument('--print_diff_stats', action='store_true', default=False)
	parser.add_argument('--save_record_count', action='store_true', default=False)
	args = parser.parse_args()
	month_end = str(monthrange(int(args.query_month[:4]), int(args.query_month[4:6]))[1])
	data_date = args.query_month+month_end
	start_time = time.mktime(datetime.strptime('2016-01-01', '%Y-%m-%d').timetuple())
	end_date = datetime.strptime('{} 23:59:59'.format(data_date), '%Y%m%d %H:%M:%S')
	end_time = time.mktime(end_date.timetuple())
	end_date = end_date.strftime('%Y-%m-%d')

	print('====> Start computation')
	pairs = getAndroidPairs(spark, data_date)

	if args.print_source_stats:
		stats = pairs.groupBy('source').agg(F.count('phone_salt').alias('record_count')).collect()
		for row in stats:
			print('----> The count for source {} is {}'.format(row['source'], row['record_count']))

	devices = getActiveAndroids(spark, data_date)
	ill_devices = getAbnormalAndroids(spark, data_date)
	devices = devices.join(ill_devices, on='imei', how='left_outer').where(F.isnull(F.col('flag'))).select('imei')
	phones = pairs.join(devices, on='imei', how='inner').select('phone_salt').distinct()
	pairs = pairs.join(phones, on='phone_salt', how='inner')

	if args.print_diff_stats:
		diffs = pairs.rdd.map(lambda row: (row['phone_salt'], row['itime'])).reduceByKey(lambda x, y: min([x, y])).map(transform2row).toDF()
		diffs = diffs.withColumn('diff', F.datediff(F.lit(end_date), F.from_unixtime(F.col('min_itime'), 'yyyy-MM-dd')))
		quantiles = diffs.approxQuantile('diff', [0.75, 0.5, 0.25], 0.02)
		for i, percentile in enumerate([0.75, 0.5, 0.25]):
			print('----> Quantile for {} is {}'.format(percentile, quantiles[i]))
		print('---->')
		print(diffs.describe('diff').show())
		print('<----')

	if args.save_record_count:
		phone_stats = pairs.rdd.map(lambda row: (row['phone_salt'], 1)).reduceByKey(add).map(lambda t: {'phone':t[0], 'count':t[1]}).map(transform2row).toDF()
		phone_stats.repartition(1).write.csv('/user/ronghui_safe/hgy/nid/phone/record_count_{}'.format(args.query_month), header=True)

	phone_stats = pairs.select(['phone_salt', 'imei']).distinct()
	phone_stats = phone_stats.rdd.map(lambda row: (row['phone_salt'], 1)).reduceByKey(add).map(lambda t: {'phone':t[0], 'imei_count':t[1]}).map(transform2row).toDF()
	phone_stats.repartition(1).write.csv('/user/ronghui_safe/hgy/nid/phone/{}'.format(args.query_month), header=True)