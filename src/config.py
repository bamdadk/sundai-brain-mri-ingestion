# config.py

import os

# Directories
# the directory
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# the src directory
SRC_DIR = os.path.join(BASE_DIR, 'src')
# the data and bids_dict file directory
DATA_DIR = os.path.join(BASE_DIR, 'data')
BIDS_DICT_DIR = os.path.join(BASE_DIR, 'data/bids_dictionary/bids_dict.csv')

