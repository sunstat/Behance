from subprocess import Popen
from subprocess import check_call

shell_file = "./shell1.sh"

args = []
args.append(shell_file)
args.append("a")

check_call(args)
