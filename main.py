#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import sys, datetime, time, os
import ibm_db
import re

from apscheduler.schedulers.blocking import BlockingScheduler

from config import app_config, ProductionConfig, DevelopmentConfig

from app.TestStateJob import TestStateJob
from app.FDCLStateJob import FDCLStateJob
from app.RCMStateJob import RCMStateJob

from app.utils.XDB2Utils import XDB2Utils


def main(p_argv):
    profile = p_argv[1] if len(p_argv) > 1 else 'dev'
    #
    config = app_config[profile]
    if os.getenv('DB_HOST') is not None:
        config.DB_HOST = os.environ.get('DB_HOST')
        config.DB_PORT = os.environ.get('DB_PORT')
        config.DB_USER = os.environ['DB_USER']
        config.DB_PASSWORD = os.environ['DB_PASSWORD']
        config.DB_INSTANCE_NAME = os.environ['DB_INSTANCE_NAME']
        config.DB_CHARSET = os.environ['DB_CHARSET']

    # test_db(p_config=config)

    scheduler = BlockingScheduler()
    # NOTE 并行 10分钟一次
    scheduler.add_job(func=run_job, args=[config], trigger='cron', minute='*/10', id="run_job", max_instances=1,
                      next_run_time=datetime.datetime.now())
    scheduler.start()

    return 0


def run_job(p_config=None):
    # UNIT_CODES = ['Q202', 'Q302', 'Q402', 'Q502', 'Q602',
    # 'Q112', 'Q212', 'Q312', 'Q412',
    # 'Q114', 'Q214', 'Q314', 'Q414']
    UNIT_CODES = ['Q402', 'Q502', 'Q412', 'Q314']
    for unit_code in UNIT_CODES:
        pattern = re.compile('Q[0-9]02')
        match = pattern.match(unit_code)
        if match:
            print('rcm--{}'.format(unit_code))
            RCMStateJob(p_config=p_config, p_unit_code=unit_code).execute()
        else:
            pattern = re.compile('Q[0-9]12')
            match = pattern.match(unit_code)
            if match:
                print('dcl--{}'.format(unit_code))
            else:
                print('fcl--{}'.format(unit_code))
            FDCLStateJob(p_config=p_config, p_unit_code=unit_code).execute()

    pass


def test_db(p_config=None):
    try:
        validation_query = 'select 1 from sysibm.sysdummy1'
        a = "DATABASE=%s;HOSTNAME=%s;PORT=%s;PROTOCOL=TCPIP;UID=%s;PWD=%s;" % (p_config.DB_INSTANCE_NAME,
                                                                               p_config.DB_HOST,
                                                                               p_config.DB_PORT,
                                                                               p_config.DB_USER,
                                                                               p_config.DB_PASSWORD)
        conn = ibm_db.connect(a, "", "")
        ok = conn is not None
        print('is DB2 connected????-->>'.format(ok))
        if conn:
            UNIT_CODE = 'Q412'
            sql = "select MAX(SD_END) from BGTARAS1.T_DWD_WH_SBOPSI_JZ_SU_QALL_0002 where UNIT_CODE='%s'" % (UNIT_CODE)

            print('\n\n?????????????????????????native start')
            print(sql)
            stmt = ibm_db.exec_immediate(conn, sql)
            results = ibm_db.fetch_both(stmt)
            print(results)
            print('?????????????????????????native end')

            print('\n\n------------>XUtils start')
            success, results = XDB2Utils.fetchall_sql(p_sql=sql, p_conn=conn)
            print(results)
            print('------------>XUtils end')

        ibm_db.close(conn)
    except Exception as e:
        print('DB2Exception in main.py')
        print(str(e))


if __name__ == '__main__':
    start = datetime.datetime.now()

    status = main(sys.argv)

    elapsed = float((datetime.datetime.now() - start).seconds)
    print("Time Used 4 All ----->>>> %f seconds" % (elapsed))

    sys.exit(status)
