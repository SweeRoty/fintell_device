# -*- coding: utf-8 -*-

from __future__ import division
from calendar import monthrange
from ConfigParser import RawConfigParser
from datetime import datetime
import argparse
import time

from pyspark import SparkConf
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import DoubleType, IntegerType

def get_samples(spark, data_date):
	sql = """
		select
			phone,
			imei,
			y,
			data_date
		from
			ronghui.yhy_model_sample
		where
			data_date = '{}'
	""".format(data_date)
	print(sql)
	samples = spark.sql(sql)
	return samples

def get_credit_scores(spark, alias, data_date):
	sql = """
		select
			imei {0},
			get_json_object(score, '$.score') score
		from
			ronghui_mart.t_model_scores_orc_bucket
		where
			dt = '{1}'
			and model = 'credit_v8'
	""".format(alias, data_date)
	print(sql)
	scores = spark.sql(sql)
	return scores

def softmax(t):
	total_score = 0.0
	partition = 0.0
	imei = t[0]
	for row in t[1]:
		total_score += row['score']*row['total_w']
		partition += row['total_w']
	return Row(imei=imei, nid_score=round(total_score/partition, 6))

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.optionxform = str
	localConf.read('../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_ID_Project___Aggregate_the_Bipartite_by_IMEI') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_month', type=str, help='The format should be YYYYmm')
	parser.add_argument('--prefix', type=str, choices=['all', 'sampled'])
	parser.add_argument('--mode', type=str, choices=['train', 'eval', 'test'])
	parser.add_argument('--print_part1', action='store_true', default=False)
	parser.add_argument('--print_part2_org', action='store_true', default=False)
	parser.add_argument('--print_part2_new', action='store_true', default=False)
	args = parser.parse_args()
	month_end = str(monthrange(int(args.query_month[:4]), int(args.query_month[4:6]))[1])
	data_date = args.query_month+month_end
	date_time = time.mktime(datetime.strptime('{} 23:59:59'.format(data_date), '%Y%m%d %H:%M:%S').timetuple())

	print('====> Start computation')
	samples = get_samples(spark, data_date)
	weights = spark.read.csv('/user/ronghui_safe/hgy/nid/weights/{}_{}_{}'.format(args.prefix, args.query_month, args.mode), header=True, inferSchema=True)
	weights = weights.withColumn('phone_salt', F.split(F.col('key'), '_').getItem(0))
	weights = weights.withColumn('imei', F.split(F.col('key'), '_').getItem(1))
	weights = weights.withColumn('itime', F.split(F.col('key'), '_').getItem(2))
	weights = weights.withColumn('itime', F.col('itime').cast(IntegerType()))
	weights = weights.withColumn('lasting_days', F.lit(date_time)-F.col('itime'))
	weights = weights.withColumn('lasting_days', F.round(F.col('lasting_days')/F.lit(24*3600), scale=2))
	weights = weights.withColumn('weight', F.pow(F.col('prediction'), F.col('lasting_days'))*F.lit(1e6))
	weights = weights.groupby(['imei', 'phone_salt']).agg(F.sum('weight').alias('weight'))
	#print(weights.describe('weight').show())
	samples = samples.join(weights, on='imei', how='left_outer')
	scores = get_credit_scores(spark, 'imei', data_date)
	scores = scores.withColumn('score', F.col('score').cast(DoubleType()))
	evaluator = BinaryClassificationEvaluator(rawPredictionCol='score', labelCol='y')

	if args.print_part1:
		samples_1 = samples.where(F.isnull(F.col('phone_salt')))
		print('----> Total count of part 1 is {}'.format(samples_1.count()))
		print('----> Distinct phone count of part 1 is {}'.format(samples_1.select('phone').distinct().count()))
		print('----> Distinct imei count of part 1 is {}'.format(samples_1.select('imei').distinct().count()))
		samples_1 = samples_1.join(scores, on='imei', how='inner')
		print('----> Total count of part 1 after join is {}'.format(samples_1.count()))
		print('----> Distinct phone count of part 1 after join is {}'.format(samples_1.select('phone').distinct().count()))
		print('----> Distinct imei count of part 1 after join is {}'.format(samples_1.select('imei').distinct().count()))
		print('----> AUC on part 1 is {:.6f}'.format(evaluator.evaluate(samples_1)))

	samples_2 = samples.where(~F.isnull(F.col('phone_salt')))
	samples_2_org = samples_2.select(['phone', 'imei', 'y']).distinct()
	if args.print_part2_org:
		print('----> Total count of part 2 is {}'.format(samples_2.count()))
		print('----> Distinct phone count of part 2 is {}'.format(samples_2.select('phone').distinct().count()))
		print('----> Distinct imei count of part 2 is {}'.format(samples_2.select('imei').distinct().count()))
		samples_2_score = samples_2_org.join(scores, on='imei', how='inner')
		print('----> Total count of part 2 after join is {}'.format(samples_2_score.count()))
		print('----> Distinct phone count of part 2 after join is {}'.format(samples_2_score.select('phone').distinct().count()))
		print('----> Distinct imei count of part 2 after join is {}'.format(samples_2_score.select('imei').distinct().count()))
		print('----> Original AUC on part 2 is {:.6f}'.format(evaluator.evaluate(samples_2_score)))

	if args.print_part2_new:
		samples_2 = samples_2.join(weights.select(F.col('phone_salt'), F.col('imei').alias('connected_imei'), F.col('weight').alias('connected_weight')), on='phone_salt', how='inner')
		samples_2 = samples_2.withColumn('w', F.col('weight')*F.col('connected_weight'))
		samples_2 = samples_2.groupby(['imei', 'connected_imei']).agg(F.sum('w').alias('total_w'))
		print('----> Total imei-imei pair count is {}'.format(samples_2.count()))
		#print('----> The stats of total_w is below: ')
		#print(samples_2.where(samples_2.total_w > 0).describe('total_w').show())
		samples_2 = samples_2.withColumn('total_w', F.when(F.col('total_w') == 0, 4.9e-324).otherwise(F.col('total_w')))
		scores = scores.select(F.col('imei').alias('connected_imei'), 'score')
		samples_2 = samples_2.join(scores, on='connected_imei', how='inner')
		samples_2 = samples_2.rdd.map(lambda row: (row['imei'], row)).groupByKey().map(softmax).toDF()
		samples_2 = samples_2.join(samples_2_org, on='imei', how='inner')
		print('----> Total count of final part 2 is {}'.format(samples_2.count()))
		print('----> Distinct phone count of final part 2 is {}'.format(samples_2.select('phone').distinct().count()))
		print('----> Distinct imei count of final part 2 is {}'.format(samples_2.select('imei').distinct().count()))
		evaluator.setRawPredictionCol('nid_score')
		print('----> New AUC on part 2 is {:.6f}'.format(evaluator.evaluate(samples_2)))