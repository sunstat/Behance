import os


'''
base directary
'''
BEHANCE_DATA_DIR = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data"
INTERMEDIATE_RESULT_DIR = "wasb://testing@adobedatascience.blob.core.windows.net/behance/IntermediateResult"
GRAPH_DIR = "wasb://testing@adobedatascience.blob.core.windows.net/behance/Graph"
BASE_DIR = "wasb://testing@adobedatascience.blob.core.windows.net/behance/IntermediateResult/base"


'''
original data path
'''
ACTION_FILE = os.path.join(BEHANCE_DATA_DIR, "action", "actionDataTrimNoView-csv")
ACTION_VIEW_FILE = os.path.join(BEHANCE_DATA_DIR, "action", "actionDataTrim-csv")
OWNER_FILE = os.path.join(BEHANCE_DATA_DIR, "owners-csv")


'''
create data path directories
'''
PID_2_INDEX_FILE = os.path.join(BASE_DIR, 'pid_2_index-csv')
PID_2_UID_FILE = os.path.join(BASE_DIR, 'pid_2_uid-csv')
PID_2_FIELD_INDEX_FILE = os.path.join(BASE_DIR, 'pid_2_field_index-csv')
FIELD_2_INDEX = os.path.join(BASE_DIR, 'field_2_index-csv')
PID_2_POPULARITY_FILE = 'pid_2_popularity-csv'
FOLLOW_MAP_FILE = 'follow_map-psv'
PID_2_SCORE_FILE = 'pid_2_score-csv'
UID_2_INDEX_FILE = 'uid_2_index-csv'
PID_2_VIEWS_FEATURE_FILE = 'pid_view_features-psv'


'''
utilities path
'''
SHELL_DIR = "../EditData/ShellEdit"
