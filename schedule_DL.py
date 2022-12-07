from Master_DL_1 import MasterExportBaseClass
from Master_DL_2 import MasterUploadFileToSF
from Super_Master_DL_3 import BaseClass
from AggrCalc import MasterAggrCalc
import sched
import time
from datetime import datetime as dt
import logging
import os
import subprocess

class run_DL_step1(MasterExportBaseClass):
    def invoke(self):
        pass

class run_DL_step2(MasterUploadFileToSF):
    def invoke(self):
        pass

class run_DL_step3(BaseClass):
    def invoke(self):
        pass

class run_AggrCalc(MasterAggrCalc):
    def invoke(self):
        pass

def run_sqlplus(sqlplus_script):
    """
    Run a sql command or group of commands against
    a database using sqlplus.
    """
    p = subprocess.Popen(['sqlplus','/nolog'],stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    (stdout,stderr) = p.communicate(sqlplus_script.encode('utf-8'))
    stdout_lines = stdout.decode('utf-8').split("\n")

    return stdout_lines
    

def get_file_directory(file):
    return os.path.dirname(os.path.abspath(file))

def remove_history_log_file(self):
    now = time.time()
    cutoff = now - (30 * 86400)  # hisotry log files are older than 30 days

    log_files = os.listdir(os.path.join(get_file_directory(__file__), "logs"))
    log_file_path = os.path.join(get_file_directory(__file__), "logs/")
    for xfile in log_files:
        if os.path.isfile(str(log_file_path) + xfile):
            t = os.stat(str(log_file_path) + xfile)
            c = t.st_ctime

            # delete file if older than 30 days
            if c < cutoff:
                os.remove(str(log_file_path) + xfile)       
    

def DL_Job(self):
    # log file
    curr_time = dt.now()
    v_filepattern = str(curr_time.year) + ('0' + str(curr_time.month))[-2:] + \
                 ('0' + str(curr_time.day))[-2:] + '-' + \
                 str(curr_time.hour) + ('0' + str(curr_time.minute))[-2:] + \
                 ('0' + str(curr_time.second))[-2:]
    filename = 'TL' + '_' + v_filepattern + '.log'

    # check if path exists, create the path if not.
    savepath = os.path.join(os.path.dirname(__file__),  'logs')
    if not os.path.exists(savepath):
        os.makedirs(savepath)

    log_file = os.path.join(savepath, filename)

    filehandler = logging.FileHandler(log_file)
    formatter = logging.Formatter(fmt='%(asctime)s  %(levelname)s  %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    filehandler.setFormatter(formatter)
    log = logging.getLogger()  # root logger
    for hdlr in log.handlers[:]:  # remove the existing file handlers
        if isinstance(hdlr, logging.FileHandler):
            log.removeHandler(hdlr)
    log.addHandler(filehandler)  # set the new handler
    # set the log level to INFO
    log.setLevel(logging.INFO)
    logging.info('Setting up log file = [%s]' % log_file)

    # TL process in three main steps
    logging.info('TL step 1: export data to csv files --- START')
    s1 = run_DL_step1()
    s1.setup_metadata()
    logging.info('TL step 1: export data to csv files --- END')

    logging.info('TL step 2: upload data file to Snowflake Stage --- START')
    s2 = run_DL_step2()
    s2.uploadFile()
    logging.info('TL step 2: upload data file to Snowflake Stage --- END')

    logging.info('TL step 3: load to Snowflake DW --- START')
    s3 = run_DL_step3()
    s3.run()
    logging.info('TL step 3: load to Snowflake DW --- END')

    logging.info('TL step 4: run Aggr Calc --- START')
    s4 = run_AggrCalc()
    s4.runAggrCalc()
    logging.info('TL step 4: run Aggr Calc --- END')
    
    # This email notifiction is depending on client's server
    # logging.info('Sending Email Notification...')
    # sqlplus_output = run_sqlplus('@d:\jesta\send_etl_success_email_prod.sql')
    # logging.info('Email Notification is sent.')
    
    remove_history_log_file(self)    

if __name__ == '__main__':
    s = sched.scheduler(time.time, time.sleep)
    now = time.time()
    s.enterabs(now + 0.01, 1, DL_Job, (0.01,))
    s.run()
    
    # single job mode in loop
    # while True:
        # if dt.now().hour in range(4, 15):
            # print(dt.now().hour)
            # s = sched.scheduler(time.time, time.sleep)
            # now = time.time()
            # s.enterabs(now + 0.01, 1, DL_Job, (0.01,))
            # s.run()
            # end_time = time.time()
            # # sleep 24 hours
            # time.sleep(86400 - (end_time - now))
        # else:
            # time.sleep(300)



