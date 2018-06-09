""" Author: Dishant Mittal
	Component of IOT-Hack
	Created on 23/1/18
	Crunches the provided raw data using spark
	"""
# from pymongo import MongoClient
# import json
import findspark
import sys
import pandas as pd
import os
import logging
findspark.init()
sys.path.append('..')
import settings
logger = logging.getLogger()
logger.setLevel(settings.COSINE_SIMILARITY['LOG_LEVEL'])

try:
	from pyspark import SparkContext
	from pyspark import SparkConf, SparkContext
except ImportError as e:
	logging.error("Can not import Spark Modules", e)
	sys.exit(1)

logging.info("Successfully imported Spark Modules")

conf = SparkConf().setMaster("local").setAppName("AggregatingMotionDeviceData")
sc = SparkContext(conf=conf)

# Script specific configurations
MINUTE_WINDOW = settings.COSINE_SIMILARITY['MINUTES_PER_WINDOW'] * 60 * 1000
BASE_TIME = settings.COSINE_SIMILARITY['BASE_TIME']
MAX_TIME = settings.COSINE_SIMILARITY['MAX_TIME']
INPUT_DIR = settings.COSINE_SIMILARITY['INPUT_DIR']
OUTPUT_DIR = '../front_end/motion_split_files_' + str(settings.COSINE_SIMILARITY['MINUTES_PER_WINDOW']) \
			 + '_mins_window/'
if not os.path.isdir(OUTPUT_DIR):
	os.makedirs(OUTPUT_DIR)


# Actual Code
def parse_line(line):
	"""
	Processes each item in rdd and returns it in an updated format.
	@param line: Item in the rdd
	@return: updated item in the rdd
	"""
	fields = line.replace(" ", "").split(',')
	global BASE_TIME
	global MAX_TIME
	if (len(fields) == 3 and fields[0] != "time" and fields[2] != '' and (long(fields[0]) - BASE_TIME >= 0L)
		and (long(fields[0]) <= MAX_TIME)):
		time_stamp = long(fields[0])
		sd = fields[1]
		device = fields[2].replace("d", "")
		if (time_stamp - BASE_TIME < long(MINUTE_WINDOW)):
			return ((int(device), BASE_TIME), float(sd))
		else:
			BASE_TIME = time_stamp
			return ((int(device), BASE_TIME), float(sd))
	else:
		return ((0, 0), 0.0)  # will eliminate these records


def drop_rows(line):
	"""
	There are some cases in the raw data where device ids don't have ids
	but just d. So they should be dropped. This method precisely accomplishes this task.
	@param line: Item in the rdd
	@return: true if string does not contain just 'd'
	"""
	fields = line.replace(" ", "").split(',')
	return fields[2] != 'd'


def calc_average(key_val):
	"""
	finding average of across each device id, basetimestamp
	@param key_val: Item in rdd
	@return: updated item with average value of sd
	"""
	sum = 0.0
	count = 0
	for elem in key_val[1]:
		sum += elem
		count += 1
	avg = sum / float(count)
	return (key_val, avg)


def change_format(key_val):
	"""
	This will convert the data to format which makes it easier for writing it to the file.
	(A decent way to write the data to csv is to convert it into simple format
	and then write it)
	@param key_val: key value pair containing <device id, basetimestamps> and <mean standard deviations>
	@return: key becomes device id and value becomes a tuple of basetimestamp and meansd
	"""
	device_id = key_val[0][0][0]
	base_timestamp = key_val[0][0][1]
	mean_sd = key_val[1]
	return (device_id, (base_timestamp, mean_sd))


def write_data(data):
	"""
	Writes the data to csv
	@param data: list
	@return: nothing
	"""
	for each_device_tuple in data:
		time_motion_list = each_device_tuple[1]
		time_motion_list = map(list, zip(
			*time_motion_list))  # way to accomplish: [(1,2,3),(4,5,6)] ->[[1,4],[2,5],[3,6]]
		timestamp = time_motion_list[0]
		standard_deviation = time_motion_list[1]
		device = [each_device_tuple[0]] * len(standard_deviation)
		df = pd.DataFrame({'device': device, 'timestamp': timestamp, 'motion': standard_deviation})
		df = df[['device', 'timestamp', 'motion']]
		# write to CSV
		if not os.path.isfile(OUTPUT_DIR + str(
				each_device_tuple[0]) + '-motion.csv'):
			df.to_csv(OUTPUT_DIR + str(
				each_device_tuple[0]) + '-motion.csv',
					  header=True, index=False)
		else:  # else file exists, so append without writing the header
			df.to_csv(OUTPUT_DIR + str(
				each_device_tuple[0]) + '-motion.csv',
					  mode='a', header=False, index=False)
		# client=MongoClient()
		# db=client['spot']
		# db.create_collection(str(each_device_tuple[0])+'-motion')
		# records = json.loads(df.T.to_json()).values()
		# db['motion_'+str(each_device_tuple[0])].insert(records)



def main():
	"""
	Reads raw sensor data. Cleans it. Crunches the data for every device. Writes it back to the filesystem.
	@return: nothing
	"""
	lines = sc.textFile(INPUT_DIR)
	cleaned = lines.filter(drop_rows)
	parsed_lines = cleaned.map(parse_line)
	cleaned_lines = parsed_lines.filter(lambda x: x[0][0] != 0)
	repartitioned_lines = cleaned_lines.repartition(1)
	grouped_by_key = repartitioned_lines.groupByKey()
	avg = grouped_by_key.map(calc_average).sortByKey()
	
	#Convert from: ((device id1,(basetime, mean_sd)),(device id1,(basetime, mean_sd)), (device id2,(basetime, mean_sd)) and so on)
	#to 
	#[
		#(device id1,[(basetime, mean_sd) , (basetime, mean_sd)]),
		#(device id2,[(basetime, mean_sd),(basetime, mean_sd)]), and so on
	#]
	output = avg.map(change_format).map(lambda (x, y): (x, [y])).reduceByKey(lambda p, q: p + q).collect()
	write_data(output)


if __name__ == "__main__":
	main()
