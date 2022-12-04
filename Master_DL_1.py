import os
import json
import datetime
import cx_Oracle
import time
import csv
import logging
import gzip
import shutil
from cryptography.fernet import Fernet

from metadata.DIM_DATE_1 import sql as dim_date_sql
from metadata.DIM_EMPL_1 import sql as dim_empl_sql
from metadata.DIM_LOC_1 import sql as dim_loc_sql
from metadata.DIM_PRD_1 import sql as dim_prd_sql
from metadata.DIM_SECTION_1 import sql as dim_section_sql
from metadata.DIM_POS_CUSTOMER_1 import sql as dim_pos_customer_sql
from metadata.DIM_POS_DEALPRICING_1 import sql as dim_pos_dealpricing_sql
from metadata.DIM_POS_DEALTYPE_1 import sql as dim_pos_dealtype_sql
from metadata.DIM_POS_DISCOUNT_1 import sql as dim_pos_discount_sql
from metadata.DIM_POS_LOYALTY_MEMBER_1 import sql as dim_pos_loyalty_member_sql
from metadata.DIM_POS_LOYALTY_PROGRAM_1 import sql as dim_pos_loyalty_program_sql
from metadata.DIM_POS_REASON_1 import sql as dim_pos_reason_sql
from metadata.DIM_POS_REQUEST_1 import sql as dim_pos_request_sql
from metadata.DIM_POS_TENDER_1 import sql as dim_pos_tender_sql
from metadata.DIM_POS_TERMINAL_1 import sql as dim_pos_terminal_sql
from metadata.DIM_POS_TRAN_TYPE_1 import sql as dim_pos_tran_type_sql
from metadata.LK_POS_RECEIPT_IMAGE_1 import sql as lk_pos_receipt_image_sql
from metadata.POS_NON_MDSE_ITEM_1 import sql as pos_non_mdse_item_sql
from metadata.FACT_POS_TRANSACTION_1 import sql as fact_pos_transaction_sql
from metadata.FACT_POS_TRAN_TENDER_1 import sql as fact_pos_tran_tender_sql
from metadata.stage_prd_1_dl import sql as stage_prd_sql
from metadata.DIM_REASON_1 import sql as dim_reason_sql
from metadata.DIM_SEASON_1 import sql as dim_season_sql
from metadata.DIM_VENDOR_1 import sql as dim_vendor_sql
from metadata.FACT_SALES_AND_MARKDOWNS_1 import sql as fact_sales_mkdn_sql
from metadata.FACT_INVENTORY_MOVEMENTS_1 import sql as fact_inv_mvmt_sql
from metadata.FACT_INVENTORY_POSITIONS_1 import sql as fact_inv_pos_sql
from metadata.FACT_PERMANENT_MARKDOWNS_1 import sql as fact_perm_mkdn_sql
from metadata.FACT_PERIOD_STOCK_LEDGER_1 import sql as fact_period_stk_ldg_sql
from metadata.FACT_SITE_WEEKLY_PLANS_1 import sql as fact_site_weekly_plan_sql
from metadata.FACT_WEEKLY_PLANS_1 import sql as fact_weekly_plan_sql
from metadata.FACT_PO_ITEMS_1 import sql as fact_purchase_order_sql
from metadata.STAGE_PO_RECEIVING_1 import sql as stage_po_receiving_sql

from metadata.REF_CHAR_TYPE_NAMES_1 import sql as ref_char_type_names_sql
from metadata.REF_CLEARANCE_STATUS_1 import sql as ref_clearance_status_sql
from metadata.REF_CURRENCIES_1 import sql as ref_currencies_sql
from metadata.REF_CURRENCY_EXCHANGE_ROUTES_1 import sql as ref_currency_exchange_routes_sql
from metadata.REF_CURRENCY_RATES_1 import sql as ref_currency_rates_sql
from metadata.REF_CURRENCY_TYPES_1 import sql as ref_currency_types_sql
from metadata.REF_CURRENCY_TYPE_AUDITS_1 import sql as ref_currency_type_audits_sql
from metadata.REF_PARAM_SELECT_PARAMS_1 import sql as ref_param_select_params_sql
from metadata.REF_PRICES_1 import sql as ref_prices_sql
from metadata.REF_SECTION_BUYERS_1 import sql as ref_section_buyers_sql
from metadata.REF_SITE_LISTS_1 import sql as ref_site_lists_sql
from metadata.REF_SITE_LIST_DETAILS_1 import sql as ref_site_list_details_sql
from metadata.REF_SPECIFIC_TAXES_1 import sql as ref_specific_taxes_sql
from metadata.REF_STYLE_COLOR_LISTS_1 import sql as ref_style_color_lists_sql
from metadata.REF_STYLE_COLOR_LIST_DETAILS_1 import sql as ref_style_color_list_details_sql
from metadata.REF_STYLE_TAX_CATEGORIES_1 import sql as ref_style_tax_categories_sql
from metadata.REF_SYSTEM_PARAMETERS_1 import sql as ref_system_parameters_sql
from metadata.REF_TAX_AUTHORITIES_1 import sql as ref_tax_authorities_sql
from metadata.REF_TAX_CATEGORIES_1 import sql as ref_tax_categories_sql
from metadata.REF_ZONE_TICKET_PRICE_1 import sql as ref_zone_ticket_price_sql
from metadata.LK_ADD_INFO_DATA_1 import sql as lk_add_info_data_sql


class ConnectionSource(cx_Oracle.Connection):

    def __init__(self, p_user_name, p_pw, p_db_name):
        # Oracle DB connection string
        connectString = p_user_name + '/' + p_pw + '@' + p_db_name
        #"stage/mprd@nrf-db-01.jestais.local/SALESDB:1521"      # "vdw/sales@nrf-db-01.jestais.local/DWSALES:1521"

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

'''
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s  %(message)s')
file_handler = logging.FileHandler('log.csv')

#file_handler.setLevel(logging.ERROR)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
#stream_handler = logging.FileHandler('log.csv')
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
#logger.addHandler(stream_handler)
'''

class MasterExportBaseClass:
    enabled_ext_group_list = []
    enabled_ext_table_list = []
    not_process_table_list = []
    ora_user_name = None
    ora_user_pw = None
    ora_db_name = None
    export_file_path = None
    export_sql = None

    sql = None
    program = None
    extract_date = None

    def __init__(self):
        pass

    def open_DB_connection(self):
        pass

    def close_DB_connection(self):
        pass

    def set_export_file_name(self, ext_group_name):
        pass

    def export_date(self):
        pass

    def invoke(self):
        self.MasterExportBaseClass()

    def setup_metadata(self):

        # self.propagate_log_msg('Using a root project directory: [%s]' % self.root)
        """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
        '''READ METADATA FILES''''''''''''''''''''''''''''''''''''''''''
        """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
        #self.sa_metadata_file = os.path.join(os.path.dirname(__file__), 'client_SF.json')
        self.sa_metadata_file = os.path.join(os.path.dirname(__file__),  'metadata', 'client_SF.json')
        print('*** file name:' + self.sa_metadata_file)
        logging.info('*** file name:' + self.sa_metadata_file)

        decode_err = ''

        try:
            str_sa_metadata = open(self.sa_metadata_file, "r").read()
            self.sa_metadata = json.loads(str_sa_metadata)
        except json.JSONDecodeError as decode_err:
            logging.error('Invalid json format. Check json file and rerun ')
            exit()
        except Exception as e:
            logging.error('Unhandled exception')
            logging.error(e)
            exit()
        else:
            pass
        finally:
            print(decode_err)
            logging.error(decode_err)

        # for key, val in self.sa_metadata.items():
        self.export_file_path = self.sa_metadata.get('FILE_PATH')
        self.ora_user_name = self.sa_metadata.get('ORA_USER_NAME')
        self.ora_user_pw = self.sa_metadata.get('ORA_USER_PW')
        key = bytes("AHBaYvXI8embdz7qgb8JvIAMN-J6Maj4ESMqd0ARdFA=", "utf-8")
        fernet = Fernet(key)
        decMessage = Fernet(key).decrypt(self.ora_user_pw.encode()).decode()
        self.ora_db_name = self.sa_metadata.get('ORA_DB_NAME')

        # get extract date, use sysdate if it is null
        self.extract_date = self.sa_metadata.get('extract_date')
        if self.extract_date is None or self.extract_date == '':
            self.extract_date = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')
        logging.info('extract date: ' + self.extract_date)

        # get extraction group setting in configure file
        self.ext_group_obj = self.sa_metadata.get('extraction_group')
        for group_item in self.ext_group_obj:
            # for group_element in group_item:
            if group_item["enabled_ind"] == 'Y' and group_item["table_name"] not in self.enabled_ext_table_list:
                self.enabled_ext_group_list.append(group_item["group_name"])
                self.enabled_ext_table_list.append(group_item["table_name"])

        logging.info(self.enabled_ext_group_list)
        logging.info(self.enabled_ext_table_list)

    #def export_data(self):
        if len(self.enabled_ext_table_list) > 0:
            try:
                # Connect to Oracle DB
                connection = ConnectionSource(self.ora_user_name, decMessage, self.ora_db_name)
                cursor = connection.cursor()
                logging.info('Source DB connection is ok')

                # Check if directory exists
                savePath = self.export_file_path
                if not os.path.exists(savePath):
                    os.makedirs(savePath)

                # the date and time will be used for  export filename
                thisdate = time.strftime("%d-%m-%Y")
                thistime = time.strftime("%H-%M-%S")
                exporttime = time.strftime("%H:%M:%S")

                # export data depending enabled_ext_group_list in order and after all extractions are done
                # TODO: in parallel? start process without waiting all extractions are done
                for table_name in self.enabled_ext_table_list:
                    self.sql = None
                    self.program = None
                    group_name = self.enabled_ext_group_list[self.enabled_ext_table_list.index(table_name)]

                    printHeader = True  # include column headers in each table output

                    reccount = 0
                    self.sql = """ select count(*)
                                from stage_vdw_process_log lg,
                                /*v_extracted_unprocess_log vlg,*/
                                stage_group_table tb
                                where lg.extract_date = to_date({v_extract_date},'YYYY-MM-DD')
                                /*and lg.extract_date = vlg.extract_date*/
                                and lg.extraction_ind ='Y' 
                                /*and lg.processed_ind='N'*/
                                and lg.group_name = tb.group_name
                                /*and vlg.group_name = tb.group_name*/
                                and lg.group_name= {v_group_name}
                    """.format(v_extract_date="'" + self.extract_date + "'", v_group_name="'" + group_name + "'")
                    print(self.sql)
                    logging.info(self.sql)

                    # self.sql = """ select count(*)
                    #             from stage_vdw_process_log lg,
                    #             v_extracted_unprocess_log vlg,
                    #             stage_group_table tb
                    #             where lg.extract_date = to_date({v_extract_date},'YYYY-MM-DD')
                    #             and lg.extract_date = vlg.extract_date
                    #             and vlg.extraction_ind ='Y'
                    #             /*and lg.processed_ind='N'*/
                    #             and lg.group_name = tb.group_name
                    #             and vlg.group_name = tb.group_name
                    #             and lg.group_name= {v_group_name}
                    # """.format(v_extract_date="'" + self.extract_date + "'", v_group_name="'" + group_name + "'")
                    cursor.execute(self.sql, ())
                    reccount = cursor.fetchone()[0]
                    print(group_name + ":" + str(reccount))
                    logging.info(group_name + ":" + str(reccount))


                    if reccount > 0:
                        # get export sql statement depending on extraction group
                        # TODO: complete the code by all extraction tables
                        # TODO: read the sql from JSON file
                        if table_name == 'DIM_LOC':
                            self.sql = dim_loc_sql
                        elif table_name == 'DIM_DATE':
                            self.sql = dim_date_sql
                        elif table_name == 'DIM_EMPL':
                            self.sql = dim_empl_sql
                        elif table_name == 'DIM_PRD':
                            # for performance, add stage tables
                            self.sql = stage_prd_sql
                            sql_add_ext_date = self.sql.replace("?",
                                                                "to_date('" + self.extract_date + "', 'YYYY-MM-DD')")
                            self.sql = sql_add_ext_date
                            logging.info(self.sql)
                            cursor.execute(self.sql, ())
                            cursor.execute('commit', ())

                            self.sql = dim_prd_sql
                        elif table_name == 'DIM_SECTION':
                            self.sql = dim_section_sql
                        elif table_name == 'DIM_POS_CUSTOMER':
                            self.sql = dim_pos_customer_sql
                        elif table_name == 'DIM_POS_DEALPRICING':
                            self.sql = dim_pos_dealpricing_sql
                        elif table_name == 'DIM_POS_DEALTYPE':
                            self.sql = dim_pos_dealtype_sql
                        elif table_name == 'DIM_POS_DISCOUNT':
                            self.sql = dim_pos_discount_sql
                        elif table_name == 'DIM_POS_LOYALTY_MEMBER':
                            self.sql = dim_pos_loyalty_member_sql
                        elif table_name == 'DIM_POS_LOYALTY_PROGRAM':
                            self.sql = dim_pos_loyalty_program_sql
                        elif table_name == 'DIM_POS_REASON':
                            self.sql = dim_pos_reason_sql
                        elif table_name == 'DIM_POS_REQUEST':
                            self.sql = dim_pos_request_sql
                        elif table_name == 'DIM_POS_TENDER':
                            self.sql = dim_pos_tender_sql
                        elif table_name == 'DIM_POS_TERMINAL':
                            self.sql = dim_pos_terminal_sql
                        elif table_name == 'DIM_POS_TRAN_TYPE':
                            self.sql = dim_pos_tran_type_sql
                        elif table_name == 'LK_POS_RECEIPT_IMAGE':
                            self.sql = lk_pos_receipt_image_sql
                        elif table_name == 'POS_NON_MDSE_ITEM':
                            self.sql = pos_non_mdse_item_sql
                        elif table_name == 'FACT_POS_TRANSACTION':
                            self.sql = fact_pos_transaction_sql
                        elif table_name == 'FACT_POS_TRAN_TENDER':
                            self.sql = fact_pos_tran_tender_sql

                        elif table_name == 'DIM_REASON':
                            self.sql = dim_reason_sql
                        elif table_name == 'DIM_SEASONS':
                            self.sql = dim_season_sql
                        elif table_name == 'DIM_VNDR':
                            self.sql = dim_vendor_sql
                        elif table_name == 'FACT_SALES_AND_MARKDOWNS':
                            self.sql = fact_sales_mkdn_sql
                        elif table_name == 'FACT_INVENTORY_MOVEMENTS':
                            self.sql = fact_inv_mvmt_sql
                        elif table_name == 'FACT_INVENTORY_POSITIONS':
                            self.sql = fact_inv_pos_sql
                        elif table_name == 'FACT_PERMANENT_MARKDOWNS':
                            self.sql = fact_perm_mkdn_sql
                        elif table_name == 'FACT_PERIOD_STOCK_LEDGER':
                            self.sql = fact_period_stk_ldg_sql
                        elif table_name == 'FACT_SITE_WEEKLY_PLANS':
                            self.sql = fact_site_weekly_plan_sql
                        elif table_name == 'FACT_WEEKLY_PLANS':
                            self.sql = fact_weekly_plan_sql
                        elif table_name == 'FACT_PO_ITEMS':
                            self.sql = fact_purchase_order_sql
                        elif table_name == 'STAGE_PO_RECEIVING':
                            self.sql = stage_po_receiving_sql
                            
                        elif table_name == 'REF_CHAR_TYPE_NAMES':
                            self.sql = ref_char_type_names_sql
                        elif table_name == 'REF_CLEARANCE_STATUS':
                            self.sql = ref_clearance_status_sql
                        elif table_name == 'REF_CURRENCIES':
                            self.sql = ref_currencies_sql
                        elif table_name == 'REF_CURRENCY_EXCHANGE_ROUTES':
                            self.sql = ref_currency_exchange_routes_sql
                        elif table_name == 'REF_CURRENCY_RATES':
                            self.sql = ref_currency_rates_sql
                        elif table_name == 'REF_CURRENCY_TYPES':
                            self.sql = ref_currency_types_sql
                        elif table_name == 'REF_CURRENCY_TYPE_AUDITS':
                            self.sql = ref_currency_type_audits_sql
                        elif table_name == 'REF_PARAM_SELECT_PARAMS':
                            self.sql = ref_param_select_params_sql
                        elif table_name == 'REF_PRICES':
                            self.sql = ref_prices_sql
                        elif table_name == 'REF_SECTION_BUYERS':
                            self.sql = ref_section_buyers_sql
                        elif table_name == 'REF_SITE_LISTS':
                            self.sql = ref_site_lists_sql
                        elif table_name == 'REF_SITE_LIST_DETAILS':
                            self.sql = ref_site_list_details_sql
                        elif table_name == 'REF_SPECIFIC_TAXES':
                            self.sql = ref_specific_taxes_sql
                        elif table_name == 'REF_STYLE_COLOR_LISTS':
                            self.sql = ref_style_color_lists_sql
                        elif table_name == 'REF_STYLE_COLOR_LIST_DETAILS':
                            self.sql = ref_style_color_list_details_sql
                        elif table_name == 'REF_STYLE_TAX_CATEGORIES':
                            self.sql = ref_style_tax_categories_sql
                        elif table_name == 'REF_SYSTEM_PARAMETERS':
                            self.sql = ref_system_parameters_sql
                        elif table_name == 'REF_TAX_AUTHORITIES':
                            self.sql = ref_tax_authorities_sql
                        elif table_name == 'REF_TAX_CATEGORIES':
                            self.sql = ref_tax_categories_sql
                        elif table_name == 'REF_ZONE_TICKET_PRICE':
                            self.sql = ref_zone_ticket_price_sql
                        elif table_name == 'LK_ADD_INFO_DATA':
                            self.sql = lk_add_info_data_sql

                        # replace question mark with extract date
                        sql_add_ext_date = self.sql.replace("?", "to_date('" + self.extract_date + "', 'YYYY-MM-DD')")
                        self.sql = sql_add_ext_date
                        print(self.sql)
                        logging.info(self.sql)

                        # start time to export data of each table
                        starttime = time.strftime("%H:%M:%S")
                        diff_start = time.time()

                        # output each table content to a separate CSV file
                        fileName = table_name + "_" + thisdate + "_" + thistime + ".csv"

                        # Create the complete filename including the absolute path
                        csv_file_dest = os.path.join(savePath, fileName)

                        outputFile = open(csv_file_dest, 'w')  # 'wb'
                        output = csv.writer(outputFile, dialect='excel')

                        cursor.execute(self.sql, ())

                        if printHeader:  # add column headers if requested
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

                        # archive the file
                        with open(csv_file_dest, 'rb') as f_in:
                            with gzip.open(os.path.join(savePath, fileName+'.gz'), 'wb') as f_out:
                                shutil.copyfileobj(f_in, f_out)
                        
                        # delete the csv file
                        os.remove(csv_file_dest)
                        
                        # archive file name
                        fileName = fileName + '.gz'

                        # end time to export data of each table
                        endtime = time.strftime("%H:%M:%S")
                        diff_end = time.time()
                        diff = diff_end - diff_start   # endtime - starttime

                        # write export result to
                        self.sql = """ insert into DL_PROCESS_STATUS (ext_date, exp_time, ext_group_name, ext_group_table_name, 
                        exp_file_name, DL1_start_time, DL1_end_time, DL1_duration, DL1_csv_rows)
                        values (to_date({v_extract_date},'YYYY-MM-DD'), {v_exp_time}, {v_group_name}, {v_table_name}, 
                        {v_file_name}, {v_starttime}, {v_endtime}, {v_duration}, {v_csv_rows})
                        """.format(v_extract_date="'" + self.extract_date + "'",
                                   v_exp_time="'" + exporttime + "'",
                                   v_group_name="'" + group_name + "'",
                                   v_table_name="'" + table_name + "'",
                                   v_file_name="'" + fileName + "'",
                                   v_starttime="'" + starttime + "'",
                                   v_endtime="'" + endtime + "'",
                                   v_duration=diff,
                                   v_csv_rows=csv_row_count)

                        cursor.execute(self.sql, ())
                        cursor.execute('commit', ())

                    else:
                        self.not_process_table_list.append(table_name)
                        continue

                # after export files are done, check if all the enabled tables are processed. If yes, update DL_PROCESS_STATUTS
                # if there is any table not being processed, write to log and send out notification.
                if self.not_process_table_list == []:
                    self.sql = """ update DL_PROCESS_STATUS
                                   set DL1_ALL_EXP_COMPLETED_STATUS = 'Y'
                                   where ext_date = to_date({v_extract_date}, 'YYYY-MM-DD')
                                   and exp_time = {v_exp_time}
                    """.format(v_extract_date="'" + self.extract_date + "'", v_exp_time="'" + exporttime + "'")
                    cursor.execute(self.sql, ())
                    cursor.execute('commit', ())
                else:
                    # TODO: write a message to log file and send email notification for the tables not being exported

                    logging.error('The following tables cannot be exported, which is either the extraction is not completed or other reasons:')
                    logging.info(self.not_process_table_list)
                    print('The following tables cannot be exported, which is either the extraction is not completed or other reasons.')
                    print(self.not_process_table_list)

                cursor.close()
                connection.close()

            except Exception as e:
                # TODO: write exception detail to log file
                logging.error("An exception occurred: " + str(e))
                print("An exception occurred:" + str(e))
                exit()
            else:
                pass
            # finally: