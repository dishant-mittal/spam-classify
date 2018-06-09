""" Author: Dishant Mittal
	Component of IOT-Hack
	Created on 23/1/18

	Sequentially combines all the csv files into a single csv
	"""

import sys
sys.path.append('..')
import settings
import logging
logger = logging.getLogger()
logger.setLevel(settings.COSINE_SIMILARITY['LOG_LEVEL'])

INPUT_PATH = '../front_end/motion_split_files_10_mins_window/inserted_missing_values/'
fout = open("../front_end/motion_split_files_10_mins_window/inserted_missing_values/merged.csv", "a")
# first file:
for line in open(INPUT_PATH + "1-motion-Normalized.csv"):
	fout.write(line)

# rest of the files:
for num in range(2, 74):
	try:
		f = open(INPUT_PATH + str(num) + "-motion-Normalized.csv")
		f.next()  # skip the header
		for line in f:
			fout.write(line)
		f.close()
	except IOError:
		logging.error(sys.exc_info()[0])
		continue;
fout.close()
