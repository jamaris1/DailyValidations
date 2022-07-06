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


class run_download(base_download_class):
    def invoke(self):
        pass

class run_formating(base_format_class):
    def invoke(self):
        pass


# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def validate(name):
    # Use a breakpoint in the code line below to debug your script.
    s1 = run_download()
    v_exporttime = s1.setup_metadata()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    validate('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
