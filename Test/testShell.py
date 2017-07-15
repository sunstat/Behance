from subprocess import Popen
from subprocess import check_call

shell_file = "./shell1.sh"

args = []
w = "asda"
args.append(shell_file)
args.append("a")
args.append(w)

check_call(args)
