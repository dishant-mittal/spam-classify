""" Author: Dishant Mittal
	Component of IOT-Hack
	Created on 23/1/18

	Computes top 10 "similar devices and corresponding days" FOR "each device and day"
	"""

import os
import csv
import sys
import time
from time import strftime
from cosine_similarity import cosine_measure
import logging
sys.path.append('..')
import settings
logger = logging.getLogger()
logger.setLevel(settings.COSINE_SIMILARITY['LOG_LEVEL'])

# script specific configurations
OUTPUT_DIR = '../front_end/motion_split_files_10_mins_window/DayWiseNormalized/'
if not os.path.isdir(OUTPUT_DIR):
	os.makedirs(OUTPUT_DIR)


def convert_timestamp(row_timestamp):
	"""
	Converts the timestamp in milliseconds to timestamp in string format
	@param row_timestamp: timestamp in milliseconds
	@return: timestamp string
	"""
	row_timestamp = int(float(row_timestamp) / 1000)
	time_obj = time.localtime(row_timestamp)
	time_str = strftime("%m-%d-%y", time_obj)
	if (strftime("%w", time_obj) == "0"):
		time_str = time_str + "-Sunday"
	elif (strftime("%w", time_obj) == "1"):
		time_str = time_str + "-Monday"
	elif (strftime("%w", time_obj) == "2"):
		time_str = time_str + "-Tuesday"
	elif (strftime("%w", time_obj) == "3"):
		time_str = time_str + "-Wednesday"
	elif (strftime("%w", time_obj) == "4"):
		time_str = time_str + "-Thursday"
	elif (strftime("%w", time_obj) == "5"):
		time_str = time_str + "-Friday"
	else:
		time_str = time_str + "-Saturday"
	return time_str


def load_values():
	"""
	Creates a list which contains an item for each device. Each item contains all the motion values sensed by that
	device on a particular day. So, 144 items for each device in total.
	This will be called just once.
	@return: list of motion values for each day for each device
	"""
	outer_arr = []
	inner_arr = []
	previous_day_str = "09-12-16-Monday"
	is_full_black_out = True
	previous_device = ""
	date_stamp_id = 1
	with open('../front_end/motion_split_files_10_mins_window/inserted_missing_values/merged.csv', 'r') as csvfile:
		reader = csv.DictReader(csvfile)
		for row in reader:
			row_timestamp = row["timestamp"]
			row_time_str = convert_timestamp(row_timestamp)
			if (row_time_str == previous_day_str):
				if (row["motion"] != "-1"):
					is_full_black_out = False
				previous_device = row["device"]
				inner_arr.append(float(row["motion"]))
			else:
				# put the last day's data in the outer array
				inner_dict = dict()
				inner_dict["day_stamp"] = previous_day_str
				inner_dict["date_stamp_id"] = str(date_stamp_id)
				date_stamp_id = date_stamp_id + 1
				inner_dict["day_data"] = inner_arr
				inner_dict["device_id"] = previous_device
				if (previous_device != row["device"]):  # as we approach new device, we startover from device 1
					date_stamp_id = 1
				if (not (is_full_black_out)):  # we don't put that day's data on which there is full day blackout
					outer_arr.append(inner_dict)
					is_full_black_out = True

				# start a new inner array to accomodate all data on this day
				inner_arr = []
				previous_day_str = row_time_str

				inner_arr.append(float(row["motion"]))
				previous_device = row["device"]
	return outer_arr


def write_to_csv(DEVICE_ID, outer_cosine_final_list):
	"""
	Writes the cosine similarity data for each device and day to the corresponding csv.
	@param DEVICE_ID: device id
	@param outer_cosine_final_list: list of cosine similarity objects
	@return: nothing
	"""
	with open(OUTPUT_DIR + DEVICE_ID + '-CosineSimilarity.csv', 'wb') as csv_file:
		field_names = ['firstdevice', 'seconddevice', 'daytimestampFirst', 'daytimestampFirstID', 'daytimestampSecond',
					  'daytimestampSecondID', 'cosinesimilarity']
		writer = csv.DictWriter(csv_file, fieldnames=field_names)
		writer.writeheader()
		for obj in outer_cosine_final_list:
			start_date = str(obj.day_timestamp_first)
			end_date = str(obj.day_timestamp_second)
			cos_value = str(obj.cosine_similarity)
			first_dev = str(obj.first_device)
			second_dev = str(obj.second_device)
			writer.writerow({'firstdevice': first_dev, 'seconddevice': second_dev, 'daytimestampFirst': start_date,
							 'daytimestampFirstID': obj.day_timestamp_first_id, 'daytimestampSecond': end_date,
							 'daytimestampSecondID': obj.day_timestamp_second_id, 'cosinesimilarity': cos_value})


def calculate_daily_cosine_sim(DEVICE_ID, outer_arr):
	"""
	Computes and write list of top 10 similar device+day combination for all 154 days for the device_id received
	@param DEVICE_ID: device id
	@param outer_arr: one time initialized array
	@return: nothing
	"""
	outer_cosine_final_list = []
	for x in range(0, len(outer_arr)):
		inner_dict_obj = outer_arr[x]
		if (inner_dict_obj["device_id"] == DEVICE_ID):  # For this device we iterate on all 154 days
			logging.info(str(inner_dict_obj["date_stamp_id"]) + " ----------------------DAYSTAMP ID-------------------- ")
			outer_temp_cosine_list = []
			for y in range(0, len(outer_arr)):
				cosine_val = cosine_measure(outer_arr[x]["day_data"], outer_arr[y]["day_data"])
				cos_obj = FinalCosineObject()
				cos_obj.day_timestamp_first = outer_arr[x]["day_stamp"]
				cos_obj.day_timestamp_second = outer_arr[y]["day_stamp"]
				cos_obj.day_timestamp_first_id = outer_arr[x]["date_stamp_id"]
				cos_obj.day_timestamp_second_id = outer_arr[y]["date_stamp_id"]
				cos_obj.cosine_similarity = cosine_val
				cos_obj.first_device = outer_arr[x]["device_id"]
				cos_obj.second_device = outer_arr[y]["device_id"]
				outer_temp_cosine_list.append(cos_obj)
			outer_temp_cosine_list.sort(key=lambda x: x.cosine_similarity, reverse=True)
			del outer_temp_cosine_list[11:]  # we are displaying only top 10 similar devices
			outer_cosine_final_list.extend(outer_temp_cosine_list)

			# outer_cosine_final_list is a List. Each 10 consecutive items inside it represent a particular
			# day and contains 10 top similar "device-days"

	write_to_csv(DEVICE_ID, outer_cosine_final_list)


class FinalCosineObject:
	day_timestamp_first = ""
	day_timestamp_second = ""
	cosine_similarity = ""
	first_device = ""
	second_device = ""
	day_timestamp_first_id = ""
	day_timestamp_second_id = ""


if __name__ == "__main__":
	outerArr = load_values()
	listArr = [x for x in settings.COSINE_SIMILARITY['DEVICE_IDS'] if
			   x not in settings.COSINE_SIMILARITY['DEVICE_IDS_ABSENT']]
	for tempVal in listArr:
		logging.info('Current Device Processing - ' + str(tempVal))
		calculate_daily_cosine_sim(str(tempVal), outerArr)
