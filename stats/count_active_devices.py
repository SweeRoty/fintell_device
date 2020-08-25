# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession

import argparse

def getActiveAndroids(spark, query_date):
	sql = """
		select
			imei
		from
			ronghui_mart.rh_online_imei_3m_android
		where
			data_date = '{0}'
	""".format(query_date)
	print(sql)
	devices = spark.sql(sql)
	return devices

def getAbnormalAndroids(spark, query_date):
	sql = """
		select
			imei,
			shanzhai_flag flag
		from
			ronghui_mart.sz_device_list
		where
			data_date = '{0}'
	""".format(query_date)
	print(sql)
	devices = spark.sql(sql)
	return devices

def getActiveIOS(spark, query_date):
	sql = """
		select
			idfa
		from
			ronghui_mart.rh_online_imei_3m_idfa
		where
			data_date = '{0}'
	""".format(query_date)
	print(sql)
	devices = spark.sql(sql)
	return devices

def getAbnormalIOS(spark, query_date):
	sql = """
		select
			idfa,
			yichang_flag flag
		from
			ronghui_mart.sz_device_list_ios
		where
			data_date = '{0}'
	""".format(query_date)
	print(sql)
	devices = spark.sql(sql)
	return devices

def transform_to_row(row_dict):
	global args
	row_dict['data_date'] = args.query_month
	return Row(**row_dict)

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.read('../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_Stats_Report___Count_Active_Devices') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_month', type=str)
	args = parser.parse_args()
	last_day = str(monthrange(int(args.query_month[:4]), int(args.query_month[4:]))[1])
	query_date = args.query_month+last_day

	print('====> Start calculation')
	active_androids = getActiveAndroids(spark, query_date)
	abnormal_androids = getAbnormalAndroids(spark, query_date)
	active_androids = active_androids.join(abnormal_androids, on=['imei'], how='left_outer').cache()

	active_ios = getActiveIOS(spark, query_date)
	abnormal_ios = getAbnormalIOS(spark, query_date)
	active_ios = active_ios.join(abnormal_ios, on=['idfa'], how='left_outer').cache()

	result = {}
	result['active_androids'] = active_androids.count()
	result['abnormal_androids'] = active_androids.where(active_androids.flag == 1).count()
	result['active_ios'] = active_ios.count()
	result['abnormal_ios'] = active_ios.where(active_ios.flag == 1).count()
	result = sc.parallelize([result]).map(transform_to_row).toDF()
	result.repartition(1).write.csv('/user/ronghui_safe/rlab/stats_report/device_stats/device_count_{0}'.format(args.query_month), header=True)