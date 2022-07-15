'''
#1 Check if there is TL execution for 6am
#2 Create a connection object for source, stage, and target.
#2 LOOP: Check counts and insert in csv
    #2.1 Check the count in source, stage, and target
    #2.3 Insert the count into a csv
#3 Perform formatting.
#4 Perform aggregate validations
#5 Send email out.
'''

from download_class import base_download_class
from formatting_class import base_format_class
import logging
import time
import os
from datetime import datetime as dt



class run_download(base_download_class):
    def invoke(self):
        pass

class run_formating(base_format_class):
    def invoke(self):
        pass

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


# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def validate(name):
    # log file
    curr_time = dt.now()
    v_filepattern = str(curr_time.year) + ('0' + str(curr_time.month))[-2:] + \
                    ('0' + str(curr_time.day))[-2:] + '-' + \
                    str(curr_time.hour) + ('0' + str(curr_time.minute))[-2:] + \
                    ('0' + str(curr_time.second))[-2:]
    filename = 'TL' + '_' + v_filepattern + '.log'

    # check if path exists, create the path if not.
    savepath = os.path.join(os.path.dirname(__file__), 'logs')
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
    # Use a breakpoint in the code line below to debug your script.
    s1 = run_download()
    s1.setup_metadata()

    s2 = run_formating()
    s2.setup_metadata()



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    validate('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
