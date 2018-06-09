""" Author: Dishant Mittal
	Component of IOT-Hack
	Created on 23/1/18

	Fills up missing values in the data crunched using spark and outputs the data in consistent format.
	"""

import csv
import pandas as pd
import os
import sys
import logging
sys.path.append('..')
import settings
logger = logging.getLogger()
logger.setLevel(settings.COSINE_SIMILARITY['LOG_LEVEL'])

# script specific configurations
INPUT_DIR = "../front_end/motion_split_files_" + str(settings.COSINE_SIMILARITY['MINUTES_PER_WINDOW']) + "_mins_window/"
OUTPUT_DIR = '../front_end/motion_split_files_' + str(settings.COSINE_SIMILARITY['MINUTES_PER_WINDOW']) + \
			 '_mins_window/inserted_missing_values/'
if not os.path.isdir(OUTPUT_DIR):
	os.makedirs(OUTPUT_DIR)
SMALLEST_VALUE = settings.COSINE_SIMILARITY['BASE_TIME']
LARGEST_VALUE = settings.COSINE_SIMILARITY['LARGEST_VALUE']
MINUTE_WINDOW = long(settings.COSINE_SIMILARITY['MINUTES_PER_WINDOW'] * 60 * 1000)
DEVICE_IDS = settings.COSINE_SIMILARITY['DEVICE_IDS']


def write_to_csv(original_df, output_file_name):
	"""
	Writes the updated list for each device to corresponding file.
	@param original_df: dataframe which needs to be written to file
	@param output_file_name: name of the output file
	@return: nothing
	"""
	original_df = original_df[['device', 'timestamp', 'motion']]

	if not os.path.isfile(output_file_name):
		original_df.to_csv(output_file_name,
						   header=True, index=False)
	else:  # else it exists, so append without writing the header
		original_df.to_csv(output_file_name,
						   mode='a', header=False, index=False)


def main():
	for i in DEVICE_IDS:
		try:
			logging.info("device = " + str(i))
			input_file_name = INPUT_DIR + str(i) + "-motion.csv"
			output_file_name = OUTPUT_DIR + str(i) + "-motion-Normalized.csv"
			time_list = range(SMALLEST_VALUE, LARGEST_VALUE, MINUTE_WINDOW)

			df = pd.DataFrame({'timestamp': time_list})
			device_list = []
			timestamp_list = []
			motion_list = []
			# within each 10 minutes duration there will be only one sensor value, since the data has been averaged
			# already for each 10 minutes interval.
			with open(input_file_name, 'r') as csvfile:
				reader = csv.DictReader(csvfile)
				for row in reader:
					timestamp = long(row["timestamp"])
					device_list.append(row["device"])
					motion_list.append(row["motion"])
					# print type(timestamp)

					if (timestamp - SMALLEST_VALUE) % MINUTE_WINDOW != 0:
						timestamp = timestamp - (timestamp - SMALLEST_VALUE) % MINUTE_WINDOW
					timestamp_list.append(timestamp)
			original_df = pd.DataFrame({'timestamp': timestamp_list, 'device': device_list, 'motion': motion_list})
			original_df.drop_duplicates('timestamp', keep='last', inplace=True)

			# merge the smaller frame into the bigger one
			original_df = original_df.set_index(['timestamp']).combine_first(df.set_index(['timestamp'])).reset_index()

			# filling the value where there isnt any value in dataframe
			original_df.motion.fillna(settings.COSINE_SIMILARITY['FILL_MISSING'], inplace=True)
			original_df.device.fillna(device_list[0], inplace=True)

			write_to_csv(original_df, output_file_name)

		except IOError:
			logging.error(sys.exc_info()[0])
			continue;


if __name__ == "__main__":
	main()
