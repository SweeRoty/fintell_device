# -*- coding: utf-8 -*-

from __future__ import division
from ConfigParser import RawConfigParser
from pyspark.ml.feature import Bucketizer
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

import argparse

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.read('../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_Stats_Report___Cal_Android_Prop_Stats') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_month', type=str)
	args = parser.parse_args()

	print('====> Start calculation')
	bucketizer = Bucketizer(splits=[0, 500, 1000, 1500, 2000, 2500, 3000, 4000, 5000, float('Inf')], \
		inputCol='price', outputCol='price_bin')

	active_devices = spark.read.csv('/user/ronghui_safe/rlab/stats_report/device_stats/device_prop_{0}' \
		.format(args.query_month), header=True).cache()
	n = active_devices.count()
	#print('^^^^^ Total number of active devices is {0}'.format(n))

	"""
	android_10_count = active_devices.select(F.col('android_version').cast('int').alias('android_version')).where(\
		F.col('android_version') == 10).count()
	print('^^^^^ Android 10 count is: {0}'.format(android_10_count))
	print('^^^^^ Android 10 ratio is: {0}'.format(round(android_10_count/n, 10)))
	"""

	isp_stats = active_devices.where((active_devices.isp.contains('移动')) | (active_devices.isp.contains('联通')) | (active_devices.isp.contains('电信')))
	isp_stats = isp_stats.groupBy(['isp']).agg(F.count(F.lit(1)).alias('isp_count')).collect()
	isp_count = 0
	for row in isp_stats:
		print('^^^^^ ISP-{0}, Count-{1}'.format(row['isp'].encode('utf-8'), row['isp_count']))
		isp_count += row['isp_count']
	print('^^^^^ ISP count is {0}'.format(isp_count))
	print('^^^^^ ISP ratio is {0}'.format(round(isp_count/n, 4)))

	active_devices = active_devices.where(active_devices.price.isNotNull()).select(F.col('price').cast('int').alias('price'))
	active_devices = bucketizer.setHandleInvalid('keep').transform(active_devices)
	price_stats = active_devices.groupBy(['price_bin']).agg(F.count(F.lit(1)).alias('price_bin_count')).collect()
	price_count = 0
	for row in price_stats:
		if row['price_bin'] != 'price_bin_None':
			print('^^^^^ PriceBin-{0}, Count-{1}'.format(row['price_bin'], row['price_bin_count']))
			price_count += row['price_bin_count']
	print('^^^^^ Price count is {0}'.format(price_count))
	print('^^^^^ Price ratio is {0}'.format(round(price_count/n, 4)))