import os
from IOutilities import IOutilities
from subprocess import Popen
output_file ='wasb://testing@adobedatascience.blob.core.windows.net/behance/IntermediateResult/2016-06-30/pid_2_popularity-csv'
delete_shell_azure = os.path.join(IOutilities.shell_dir, 'delete.sh')
Popen('./%s %s' % (delete_shell_azure, output_file,), shell=True)