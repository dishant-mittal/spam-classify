""" Author: Dishant Mittal
	Component of IOT-Hack
	Created on 23/1/18

	Finds the Cosine Similarity for each device with every other device.
	"""

import os
import csv
import sys
import math
from itertools import izip
import logging
sys.path.append('..')
import settings
logger = logging.getLogger()
logger.setLevel(settings.COSINE_SIMILARITY['LOG_LEVEL'])

# script specific configurations
INPUT_DIR = "../front_end/motion_split_files_" + str(settings.COSINE_SIMILARITY['MINUTES_PER_WINDOW']) + \
			"_mins_window/inserted_missing_values/"
OUTPUT_DIR = '../front_end/motion_split_files_' + str(settings.COSINE_SIMILARITY['MINUTES_PER_WINDOW']) + \
			 '_mins_window/CosineSimilarity_Normalized/'
if not os.path.isdir(OUTPUT_DIR):
	os.makedirs(OUTPUT_DIR)
	logging.info('directory created')


def dot_product(v1, v2):
	"""
	Computes dot products of the to vectors received
	@param v1: first vector
	@param v2: second vector
	@return: dot product
	"""
	return sum(map(lambda x: x[0] * x[1], izip(v1, v2)))  # izip does the similar thing as segregate motion..izip..


def cosine_measure(v1, v2):
	"""
	Compute Cosine Similarity between two vectors
	@param v1: First vector
	@param v2: Second vector
	@return: Cosine Similarity
	"""
	prod = dot_product(v1, v2)
	len1 = math.sqrt(dot_product(v1, v1))
	len2 = math.sqrt(dot_product(v2, v2))
	return prod / (len1 * len2)


def write_data(SOURCE_DEVICE_VAL, list_cos_obj, output_file_name):
	"""
	Writes data to the file
	@param SOURCE_DEVICE_VAL: The device id for which currently writing
	@param list_cos_obj: list object
	@param output_file_name: name of the output file
	@return:
	"""
	TOP_COUNT = settings.COSINE_SIMILARITY['TOP_COUNT']
	list_cos_obj.sort(key=lambda x: x.computed_metric, reverse=True)  # O(n logn)
	with open(output_file_name, 'wb') as csv_file:
		field_names = ['SourceDevice', 'ComparedDevice', 'CosineSim', 'FeatureVectorLength', 'ComputedMetric']
		writer = csv.DictWriter(csv_file, fieldnames=field_names)
		writer.writeheader()
		source_device = str(SOURCE_DEVICE_VAL)
		for cos_obj_print in list_cos_obj:
			if (TOP_COUNT > 0):
				writer.writerow({'SourceDevice': source_device, 'ComparedDevice': cos_obj_print.device_id,
								 'CosineSim': cos_obj_print.cosine_sim,
								 'FeatureVectorLength': cos_obj_print.len_feature_vector,
								 'ComputedMetric': cos_obj_print.computed_metric})
				TOP_COUNT = TOP_COUNT - 1


def main(SOURCE_DEVICE_VAL):
	"""
	Invokes code for computing Cosine Similarity for each device w.r.t. each other device
	@param SOURCE_DEVICE_VAL: The device for which currently computing cosine similarities
	@return: nothing
	"""
	# create directory
	logging.info(SOURCE_DEVICE_VAL)
	device_feature_vector = []
	cosine_map = {}
	device_file_name = INPUT_DIR + str(SOURCE_DEVICE_VAL) + "-motion-Normalized.csv"
	output_file_name = OUTPUT_DIR + str(SOURCE_DEVICE_VAL) + "-motion-Normalized-CosineOccupancy.csv"
	if SOURCE_DEVICE_VAL not in settings.COSINE_SIMILARITY['DEVICE_IDS_ABSENT']:
		try:
			with open(device_file_name, 'r') as csv_file:
				reader = csv.DictReader(csv_file)
				for row in reader:  # compute the vector for this device
					float_val = float(row["motion"])
					device_feature_vector.append(float_val)
		except IOError:
			logging.error(sys.exc_info()[0])

		for i in settings.COSINE_SIMILARITY['DEVICE_IDS']:
			testdevice_feature_vector = []
			try:
				device_id = str(i)
				test_device_file_name = INPUT_DIR + device_id + "-motion-Normalized.csv"
				logging.info("device - " + device_id)
				with open(test_device_file_name, 'r') as csv_file:
					reader = csv.DictReader(csv_file)
					for row in reader:  # compute the vector for this device
						float_val2 = float(row["motion"])
						testdevice_feature_vector.append(float_val2)
				device_feature_vector_final = []
				testdevice_feature_vector_final = []

				# if any value for any device is present, then append the values for both the device in the list
				for z in range(0, len(device_feature_vector)):
					if (device_feature_vector[z] == -1 or testdevice_feature_vector[z] == -1):
						continue;
					else:
						testdevice_feature_vector_final.append(testdevice_feature_vector[z])
						device_feature_vector_final.append(device_feature_vector[z])

				if (len(device_feature_vector_final) > 0):  # this condition should always be satisfied else cosine would have 0 in denominator
					cosine_similarity = cosine_measure(device_feature_vector_final, testdevice_feature_vector_final)
					val_output = str(cosine_similarity) + "####" + str(len(device_feature_vector_final))
					cosine_map[device_id] = val_output
				else:
					cosine_map[device_id] = str(0) + "####" + str(0)


			except IOError:
				logging.error(sys.exc_info()[0])
				continue;

	list_cos_obj = []
	for key, value in cosine_map.items():
		cos_obj = CosineSimMap()
		cos_obj.device_id = str(key)
		starting_time_stamp = str(value)
		cos_obj.cosine_sim = starting_time_stamp.split("####")[0]
		cos_obj.len_feature_vector = starting_time_stamp.split("####")[1]
		cos_obj.computed_metric = float(cos_obj.cosine_sim) * float(cos_obj.len_feature_vector)
		list_cos_obj.append(cos_obj)

	write_data(SOURCE_DEVICE_VAL, list_cos_obj, output_file_name)


class CosineSimMap:
	"""
	Defines type for each row which needs to be written in CSV
	"""
	device_id = "0"
	cosine_sim = "0"
	len_feature_vector = 0
	computed_metric = 0.0


if __name__ == "__main__":
	listArr = settings.COSINE_SIMILARITY['DEVICE_IDS']
	for tempVal in listArr:
		main(tempVal)
