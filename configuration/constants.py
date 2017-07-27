import os


'''
base directary
'''
BEHANCE_DATA_DIR = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data"
INTERMEDIATE_RESULT_DIR = "wasb://testing@adobedatascience.blob.core.windows.net/behance/IntermediateResult"
BASE_DIR = "wasb://testing@adobedatascience.blob.core.windows.net/behance/IntermediateResult/base"
TEMPORARY_DIR =  "wasb://testing@adobedatascience.blob.core.windows.net/behance/TemporaryFile"
MODEL_DIR = "wasb://testing@adobedatascience.blob.core.windows.net/behance/Models"
MODEL_LOG_DIR = "wasb://testing@adobedatascience.blob.core.windows.net/behance/Models/Log"


'''
original data path
'''
ACTION_FILE = os.path.join(BEHANCE_DATA_DIR, "action", "actionDataTrimNoView-csv")
ACTION_VIEW_FILE = os.path.join(BEHANCE_DATA_DIR, "action", "actionDataTrim-csv")
OWNER_FILE = os.path.join(BEHANCE_DATA_DIR, "owners-csv")
IMAGE_FILE = os.path.join(BEHANCE_DATA_DIR, 'image-url-csv')
IMAGE_TRIMMED_FILE = os.path.join(BEHANCE_DATA_DIR, 'image-trimmed_url-csv')


'''
create data path directories
'''
PID_2_INDEX_FILE = os.path.join(BASE_DIR, 'pid_2_index-csv')
PID_2_UID_FILE = os.path.join(BASE_DIR, 'pid_2_uid-csv')
PID_2_FIELD_INDEX_FILE = os.path.join(BASE_DIR, 'pid_2_field_index-psv')
FIELD_2_INDEX = os.path.join(BASE_DIR, 'field_2_index-csv')
UID_2_INDEX_FILE = os.path.join(BASE_DIR, 'uid_2_index-csv')
PID_2_POPULARITY_FILE = os.path.join(BASE_DIR, 'pid_2_popularity-csv')
FOLLOW_MAP_FILE = os.path.join(BASE_DIR, 'follow_map-psv')
PID_2_SCORE_FILE = os.path.join(BASE_DIR, 'pid_2_score-csv')
PID_2_VIEWS_FEATURE_FILE = os.path.join(BASE_DIR, 'pid_2_view_features-psv')
TRAIN_PID_SET_FILE = os.path.join(BASE_DIR, 'pid_train')
VALID_PID_SET_FILE = os.path.join(BASE_DIR, 'pid_valid')
TEST_PID_SET_FILE = os.path.join(BASE_DIR, 'pid_test')
TRAIN_PID_SAMPLE_SET_FILE = os.path.join(BASE_DIR, 'pid_sample_train')
VALID_PID_SAMPLE_SET_FILE = os.path.join(BASE_DIR, 'pid_sample_valid')
TEST_PID_SAMPLE_SET_FILE = os.path.join(BASE_DIR, 'pid_sample_test')
PID_2_DATE_FILE = os.path.join(BASE_DIR, 'pid_2_date-csv')
PID_2_VIEWS_FEATURE_FILE = os.path.join(BASE_DIR, 'pid_2_view_feature-psv')

'''
utilities path
'''
SHELL_DIR = "/home/yiming/Behance/EditData/ShellEdit"
GRAPH_DIR = "/home/yiming/Behance/Graph"