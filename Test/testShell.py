import os
from subprocess import Popen
from subprocess import check_call

shell_file = "shell1.sh"

a1 = "hello"
a2 = "world"


args = ['/usr/bin/env bash']
args.append(shell_file)
args.append(a1)
args.append(a2)

print ' '.join(args)
print '{} {} {} {}'.format('/usr/bin/env bash', shell_file, a1, a2)

a = Popen(' '.join(args), shell=True)
#print a.wait()



'''
a = Popen('{} {} {} {}'.format('/usr/bin/env bash', shell_file, a1, a2), shell =True)
print a.wait()
'''




