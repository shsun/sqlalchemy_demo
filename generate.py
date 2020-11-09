#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import os


def main(p_argv):
    # 生成指定模型
    # cmd = "sqlacodegen --tables account mysql+pymysql://root:123456@192.168.1.14/management >models.py"

    # 生成全部模型
    cmd = "sqlacodegen mysql+pymysql://root:123456@192.168.1.14/management >models.py"
    os.system(cmd)

    return 0


if __name__ == '__main__':
    start = datetime.datetime.now()

    status = main(sys.argv)

    elapsed = float((datetime.datetime.now() - start).seconds)
    print("Time Used 4 All ----->>>> %f seconds" % (elapsed))

    sys.exit(status)
