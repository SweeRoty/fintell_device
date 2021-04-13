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
	localConf.read('../../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_ID_Project___Extract_Sample_from_Sequence') \
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

	start_time = time.mktime(datetime.strptime('20160101', '%Y%m%d').timetuple())
	month_end = str(monthrange(int(args.query_month[:4]), int(args.query_month[4:6]))[1])
	data_date = args.query_month + month_end
	end_time = time.mktime(datetime.strptime('{} 23:59:59'.format(data_date), '%Y%m%d %H:%M:%S').timetuple())
	fr_start_time = time.mktime(datetime.strptime('{}01'.format(args.query_month), '%Y%m%d').timetuple())

	print('====> Start computation')
	pairs = getAndroidPairs(spark, data_date)
	phones = spark.read.csv('/user/ronghui_safe/hgy/nid/phones/{}'.format(args.query_month), header=True)
	if args.mode != 'test':
		phones = phones.where(F.col('imei_count').between(2, 20)) #120
		phones = phones.select(F.col('phone').alias('phone_salt')).sample(False, 0.1, 11267)
		pairs = pairs.join(phones, on='phone_salt', how='inner')
	else:
		phones_1 = phones.where(F.col('imei_count').between(1, 20)) #120
		phones_1 = phones_1.select(F.col('phone').alias('phone_salt'))
		pairs = pairs.join(phones_1, on='phone_salt', how='inner')
		"""
		phones_2 = phones.where(F.col('imei_count') > 120)
		phones_2 = phones_2.select(F.col('phone').alias('phone_salt'))
		pairs_2 = pairs.join(phones_2, on='phone_salt', how='inner')
		pairs_2_stats = pairs_2.groupby(['phone_salt', 'imei']).agg(F.max('itime').alias('latest_time'))
		pairs_2_stats = pairs_2_stats.withColumn('row_num', F.row_number().over(Window.partitionBy('phone_salt').orderBy(F.col('latest_time').desc())))
		pairs_2_stats = pairs_2_stats.where(F.col('row_num') <= 120)
		pairs_2 = pairs_2.join(pairs_2_stats.select(['phone_salt', 'imei']), on=['phone_salt', 'imei'], how='inner').select(pairs_1.columns)
		pairs = pairs_1.union(pairs_2)
		"""
	samples = None
	phone_min_itime = pairs.rdd \
							.map(lambda row: (row['phone_salt'], row['itime'])) \
							.reduceByKey(lambda x, y: min(x, y)) \
							.map(lambda t: Row(phone_salt=t[0], min_itime=t[1])) \
							.toDF()
	if args.mode != 'test':
		samples = pairs.rdd.map(lambda row: (row['phone_salt'], row)).groupByKey().flatMap(generateLabels).toDF()
		phone_min_itime = phone_min_itime.where(F.col('min_itime') >= start_time+365*24*3600)
		samples = samples.join(phone_min_itime, on='phone_salt', how='inner')
	else:
		samples = pairs.withColumn('duration', F.lit(-1))
		samples = samples.where(F.col('itime') >= fr_start_time).join(phone_min_itime, on='phone_salt', how='inner')
		samples = samples.join(phone_min_itime, on='phone_salt', how='inner')
		samples = samples.select(['phone_salt', 'duration', 'imei', 'itime', 'source', 'min_itime'])
	samples.repartition(10).write.csv('/user/ronghui_safe/hgy/nid/samples/{}_{}'.format(args.query_month, args.mode), header=True)