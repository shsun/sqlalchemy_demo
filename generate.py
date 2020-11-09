import os
 
cmd = "sqlacodegen  mysql+pymysql://root:123456@192.168.1.14/management >tmp.py"

os.system(cmd)

