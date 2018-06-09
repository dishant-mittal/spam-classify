""" Author: Dishant Mittal
    Component of IOT-Hack
    Created on 23/1/18

    Project Configurations
    """
import logging

COSINE_SIMILARITY = {
    'LOG_LEVEL': logging.ERROR, # DEBUG, INFO, WARNING, ERROR, CRITICAL
    'BASE_TIME': 1473732300000L,  # this serves as the first basetimestamp
    'MINUTES_PER_WINDOW': 10,
    'INPUT_DIR': "../original_data/Motions.csv",
    'LARGEST_VALUE': 1487012700000L,
    'MAX_TIME': 1487048400000L - 30000L,
    'FILL_MISSING': -1.0,
    'TOP_COUNT': 10,  # to just write the attributes calculated with respect to top 10 devices
    'DEVICE_IDS': [x for x in range(1, 74)],
    'DEVICE_IDS_ABSENT': [3, 32, 47, 48, 55, 59, 60]
}
