import time

import cx_Oracle
import snowflake.connector
import logging
import os
import csv

from source_query_FIN import sql as fin_source_query
from stage_query_FIN import sql as fin_stage_query
from target_query_FIN import sql as fin_target_query

class ConnectionSource(cx_Oracle.Connection):

    def __init__(self,connectString):
        return super(ConnectionSource, self).__init__(connectString)

    def cursor(self):
        return CursorSource(self)


class CursorSource(cx_Oracle.Cursor):

    def execute(self, statement, args):
        for argIndex, arg in enumerate(args):
            print("   ", argIndex + 1, "=>", repr(arg))
        return super(CursorSource, self).execute(statement, args)

    def fetchone(self):
        return super(CursorSource, self).fetchone()

    def close_cursor(self):
        return super(CursorSource.close_cursor())

    def invoke(self):
        pass






class base_download_class:

    DBConnections = {'fin_source_query': 'vfin/mprd@10.20.1.19/mprd', 'fin_stage_query': 'stage/mprd@10.20.1.19/mprd','fin_target_query': 'stage/mprd@10.20.1.19/mprd'}
    sql = None



    def __init__(self):
        pass

    def invoke(self):
        self.base_download_class()

    def setup_metadata(self):

        try:
            for key, value in self.DBConnections.items():
                connection = ConnectionSource(value)
                cursor = connection.cursor()
                logging.info('Source DB connection is ok')

                if key == 'fin_source_query':
                    logging.info('Executing fin_source_query')
                    self.sql = fin_source_query
                    logging.info(self.sql)

                elif key == 'fin_target_query':
                    #time.sleep(10800)


                    ctx = snowflake.connector.connect(
                        user= 'chrspo_etl',
                        password= 'chrspo123',
                        account= 'jestais.east-us-2.azure'
                    )
                    cs = ctx.cursor()

                    cs.execute("""USE WAREHOUSE CHRSPO_FIN_WH""")
                    cs.execute("""USE DATABASE CHRSPO_DB_PRD""")
                    cs.execute("""USE SCHEMA FIN""")

                    logging.info('Executing fin_target_query')
                    self.sql = fin_target_query
                    logging.info(self.sql)

                    cs.execute(self.sql)


                else:
                    logging.info('Executing fin_stage_query')
                    self.sql = fin_stage_query
                    logging.info(self.sql)


                savePath = 'C:\\Users\\jamaris\\PycharmProjects\\DailyValidations\\files'

                # output each table content to a separate CSV file
                fileName = key + ".csv"

                # Create the complete filename including the absolute path
                csv_file_dest = os.path.join(savePath, fileName)

                outputFile = open(csv_file_dest, 'w')  # 'wb'
                output = csv.writer(outputFile, dialect='excel', lineterminator="\n")


                if key == 'fin_source_query' or key == 'fin_stage_query':


                    cursor.execute(self.sql, ())

                    cols = []
                    for col in cursor.description:
                        cols.append(col[0])
                    output.writerow(cols)

                    # row count
                    csv_row_count = 0

                    for row_data in cursor:  # add table rows
                        output.writerow(row_data)
                        csv_row_count = csv_row_count + 1
                    outputFile.close()

                elif key == 'fin_target_query':

                    cols = []
                    for col in cs.description:
                        cols.append(col[0])
                    output.writerow(cols)

                    for row_data in cs:
                        output.writerow(row_data)
                        csv_row_count = csv_row_count +1
                    outputFile.close()

        except cx_Oracle.DatabaseError as decode_err:
            logging(decode_err)




