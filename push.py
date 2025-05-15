r"""

git config --global credential.helper store
git config --global user.email "fordicus@naver.com"
git pull
git add *
git commit -m init
git push origin main

"""

from   	datetime 	import datetime
from	getmac		import get_mac_address as gma
from 	subprocess 	import Popen, PIPE
import 	pathlib
import 	time
import 	sys
import 	os

print('---------------------------------------------------------------------------')

F = os.listdir(pathlib.Path().absolute())

I = [	# delete if it exist in the local machine (keep it in the local machine)
	'desktop.ini'
]

J = [	# delete even if it does not exist in local machine
]

os.system('git config --global credential.helper store')
os.system('git config --global user.email "fordicuso@gmail.com"')

os.system('git pull')

if '__pycache__' in F: 
	os.system('rm -rf __pycache__/')


os.system('git add *')

for f in F:

	if f == '.vs': 			os.system('git rm --cached .vs/ -r')
	if f == 'x64': 			os.system('git rm --cached x64/ -r')


if len(I) > 0:

	for i in I:

		for f in F:

			if i == f:

				os.system('git rm --cached %s' % i)
				break

if len(J) > 0:

	for j in J:

		os.system('git rm --cached %s' % j)

gma_short   = gma().split(':')[-3:]
commit_msg  = f"{'-'.join(gma_short)} {datetime.now()}"
os.system(f'git commit -m "{commit_msg}: "')
#os.system(f"""git commit -m "{gma()} {str(datetime.now())}: " """)
os.system("git push origin main")
os.system("git log -1")