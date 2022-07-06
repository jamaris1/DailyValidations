import cx_Oracle
import logging


class ConnectionSource(cx_Oracle.Connection):

    def __init__(self):
        # Oracle DB connection string
        connectString =  'stage/mprd@10.20.1.19/mprd'
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

    def invoke(self):
        pass


class base_download_class:

    DBConnections = {'source': 'vfin/mprd@10.20.1.19/mprd', 'stage': 'stage/mprd@10.20.1.19/mprd', 'target': 'stage'}
    sql = None



    def __init__(self):
        pass

    def invoke(self):
        self.base_download_class()

    def setup_metadata(self):

        try:
            connection = ConnectionSource()
            cursor = connection.cursor()
            logging.info('Source DB connection is ok')
            cursor.execute(self.sql, ())
            reccount = cursor.fetchone()[0]

            #if reccount > 0:
            #    # output each table content to a separate CSV file
            #    fileName = table_name + "_" + thisdate + "_" + thistime + ".csv"

            #    # Create the complete filename including the absolute path
            #    csv_file_dest = os.path.join(savePath, fileName)

            #    outputFile = open(csv_file_dest, 'w')  # 'wb'
            #    output = csv.writer(outputFile, dialect='excel', lineterminator="\n")
        except cx_Oracle.DatabaseError as decode_err:
            logging(decode_err)




