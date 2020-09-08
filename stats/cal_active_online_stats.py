# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession

import argparse

def retrieveUidInfo(spark, to, os):
	device = 'imei' if os == 'a' else 'idfa'
	sql = """
		select
			distinct uid,
			package_name app_package,
			{0} device_id
		from
			ronghui.register_user_log
		where
			data_date <= '{1}'
			and platform = '{2}'
	""".format(device, to, os)
	print(sql)
	uids = spark.sql(sql)
	return uids

def retrieveUidIDFA(spark, to):
	sql = """
		select
			distinct uid,
			idfa device_id,
			app_key
		from
			ronghui.uid2idfa_fact
		where
			data_date <= '{0}'
	""".format(to)
	print(sql)
	uids = spark.sql(sql)
	return uids

def retrieveAppInfo(spark):
	sql = """
		select
			package app_package,
			app_key
		from
			ronghui_mart.app_info
	"""
	print(sql)
	apps = spark.sql(sql)
	return apps

def getDailyStats(spark, tb, fr, to, os):
	sql = """
		select
			md5(cast(uid as string)) uid,
			stat_uid_act_cnt active_count_per_day
		from
			ronghui_mart.rh_base_user_{0}_uid
		where
			data_date between '{1}' and '{2}'
			--and from_unixtime(itime, 'yyyyMMdd') between '{1}' and '{2}'
			and platform = '{3}'
			and stat_uid_act_cnt > 0
	""".format(tb, fr, to, os)
	print(sql)
	stats = spark.sql(sql)
	return stats

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.read('../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_Stats_Report___Cal_Active_Online_Stats') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_month', type=str)
	parser.add_argument('--os', choices=['a', 'i'])
	parser.add_argument('--kind', choices=['active', 'online'])
	args = parser.parse_args()
	fr = args.query_month+'01'
	to = args.query_month+str(monthrange(int(args.query_month[:4]), int(args.query_month[4:]))[1])
	tb = '01' if args.kind == 'active' else '02'

	print('====> Start calculation')
	uids = retrieveUidInfo(spark, to, args.os)
	if args.os == 'i':
		uid_idfa = retrieveUidIDFA(spark, to)
		apps = retrieveAppInfo(spark)
		uid_idfa = uid_idfa.join(apps, on=['app_key'], how='inner').drop('app_key').select(['uid', 'app_package', 'device_id'])
		uids = uids.union(uids)

	stats = getDailyStats(spark, tb, fr, to, args.os)
	stats = stats.join(uids, on=['uid'], how='inner')

	stats = stats.repartition(20000, ['device_id']).groupBy(['device_id']).agg( \
		F.sum('active_count_per_day').alias('total_times'), \
		F.countDistinct('app_package').alias('total_apps'))
	stats = stats.withColumn('avg_app_times', F.col('total_times')/F.col('total_apps'))

	stats = stats.select( \
		F.sum('total_times').alias('total_{0}_times'.format(args.kind)), \
		F.count(F.lit(1)).alias('total_{0}_devices'.format(args.kind)), \
		F.mean('total_times').alias('avg_{0}_times_per_device'.format(args.kind)), \
		F.mean('total_apps').alias('avg_{0}_apps_per_device'.format(args.kind)), \
		F.mean('avg_app_times').alias('avg_{0}_times_per_app_per_device'.format(args.kind)))
	stats.write.csv('/user/ronghui_safe/hgy/stats_report/{0}/{1}/{2}'.format(args.kind, args.query_month, args.os), header=True)