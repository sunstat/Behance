import os
BEHANCE_DATA_DIR = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data"
INTERMEDIATE_RESULT_DIR = "wasb://testing@adobedatascience.blob.core.windows.net/behance/IntermediateResult"
BASE_DIR = "wasb://testing@adobedatascience.blob.core.windows.net/behance/IntermediateResult/base"
PID_2_INDEX_FILE = os.path.join(BASE_DIR, 'pid_2_index-csv')
PID_2_PID_FILE = os.path.join(BASE_DIR, 'pid_2_uid-csv')
PID_2_FIELD_INDEX_FILE = os.path.join(BASE_DIR, 'pid_2_field_index-csv')
FIELD_2_INDEX = os.path.join(BASE_DIR, 'field_2_index-csv')
PID_2_POPULARITY_FILE = 'pid_2_popularity-csv'
FOLLOW_MAP_FILE = 'follow_map-psv'
PID_2_SCORE_FILE = 'pid_2_score-csv'