import os

os.chdir(os.getcwd())
print os.getcwd()
os.system('md aaa')
for root, dirs, files in os.walk('.'):
    for file in files:
        if file.endswith('.ui'):
            str = 'pyuic4 -o ui_%s.py %s' % (file.rsplit('.', 1)[0], file)
            print str
            os.system(str)
            # os.system(str)
            # os.system('pyuic4 -o ui_%s.py %s' % (file.rsplit('.', 1)[0], file))
        elif file.endswith('.qrc'):
            pass
			# os.system('pyrcc4 -o %s_rc.py %s' % (file.rsplit('.', 1)[0], file))