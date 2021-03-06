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
			edw.jiguang_phone_encrypt
		where
			data_date <= '{0}'
			and phone_salt is not null
			and imei is not null
			and itime between {1} and {2}
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
	localConf.read('./config')
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
	args = parser.parse_args()
	month_end = str(monthrange(int(args.query_month[:4]), int(args.query_month[4:6]))[1])
	data_date = args.query_month+month_end
	start_time = time.mktime(datetime.strptime('2016-01-01', '%Y-%m-%d').timetuple())
	end_time = time.mktime(datetime.strptime('{} 23:59:59'.format(data_date), '%Y%m%d %H:%M:%S').timetuple())

	print('====> Start computation')
	pairs = getAndroidPairs(spark, data_date)
	devices = getActiveAndroids(spark, data_date)
	ill_devices = getAbnormalAndroids(spark, data_date)
	devices = devices.join(ill_devices, on='imei', how='left_outer').where(F.isnull(F.col('flag'))).select('imei')
	phones = pairs.join(devices, on='imei', how='inner').select('phone_salt').distinct()
	pairs = pairs.join(phones, on='phone_salt', how='inner')

	phone_min_itime = pairs.rdd \
							.map(lambda row: (row['phone_salt'], row['itime'])) \
							.reduceByKey(lambda x, y: min(x, y)) \
							.map(lambda t: Row(phone_salt=t[0], min_itime=t[1])) \
							.toDF()

	phone_stats = pairs.select(['phone_salt', 'imei']).distinct()
	phone_stats = phone_stats.rdd.map(lambda row: (row['phone_salt'], 1)).reduceByKey(add).map(lambda t: {'phone_salt':t[0], 'imei_count':t[1]}).map(transform2row).toDF()
	phone_stats = phone_stats.join(phone_min_itime, on='phone_salt', how='inner')
	phone_stats = phone_stats.select(['phone_salt', 'imei_count', 'min_itime'])
	phone_stats = phone_stats.registerTempTable('temp')
	spark.sql('''INSERT OVERWRITE TABLE tmp.step1_phone PARTITION (data_date = '{0}') SELECT * FROM temp'''.format(args.query_month)).collect()