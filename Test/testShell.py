from subprocess import Popen

shell_file = "shell1.sh"



Popen('./%s '%(shell_file,), shell=True)