sql = '''
SELECT 'DW_SF_FIN_ADDRESS' AS TABLE_NAME, COUNT(*) AS STAGE_COUNT FROM DW_FIN_ADDRESS
UNION ALL
SELECT 'DW_SF_FIN_ADDRESS_TYPE', COUNT(*) FROM DW_FIN_ADDRESS_TYPE
UNION ALL
SELECT 'DW_SF_FIN_AGING_BUCKET', COUNT(*) FROM DW_FIN_AGING_BUCKET
UNION ALL
SELECT 'DW_SF_FIN_APP_OBJECT_SECURITY', COUNT(*) FROM DW_FIN_APP_OBJECT_SECURITY
UNION ALL
SELECT 'DW_SF_FIN_AP_AGING_TRANSACTION', COUNT(*) FROM DW_FIN_AP_AGING_TRANSACTION WHERE IUD_FLAG = 'I'
UNION ALL
SELECT 'DW_SF_FIN_AP_AGING_TRANS_STMT', COUNT(*) FROM DW_FIN_AP_AGING_TRANS_STMT
UNION ALL
SELECT 'DW_SF_FIN_AP_SUMMARY', COUNT(*) FROM DW_FIN_AP_SUMMARY
UNION ALL
SELECT 'DW_SF_FIN_AP_SUMMARY_REPORT', COUNT(*) FROM DW_FIN_AP_SUMMARY_REPORT
UNION ALL
SELECT 'DW_SF_FIN_AR_AGING', COUNT(*) FROM DW_FIN_AR_AGING
UNION ALL
SELECT 'DW_SF_FIN_AR_AGING_TRANSACTION', COUNT(*) FROM DW_FIN_AR_AGING_TRANSACTION
UNION ALL
SELECT 'DW_SF_FIN_ATTACHMENT', COUNT(*) FROM DW_FIN_ATTACHMENT
UNION ALL
SELECT 'DW_SF_FIN_BANK', COUNT(*) FROM DW_FIN_BANK
UNION ALL
SELECT 'DW_SF_FIN_BANK_ACCOUNT', COUNT(*) FROM DW_FIN_BANK_ACCOUNT
UNION ALL
SELECT 'DW_SF_FIN_BANK_ACCOUNT_LOT', COUNT(*) FROM DW_FIN_BANK_ACCOUNT_LOT
UNION ALL
SELECT 'DW_SF_FIN_BANK_ADJUSTMENT', COUNT(*) FROM DW_FIN_BANK_ADJUSTMENT
UNION ALL
SELECT 'DW_SF_FIN_BANK_ADJ_DISTRIBUTION', COUNT(*) FROM DW_FIN_BANK_ADJ_DISTRIBUTION
UNION ALL
SELECT 'DW_SF_FIN_BANK_ADJ_EXCHANGE_RATE', COUNT(*) FROM DW_FIN_BANK_ADJ_EXCHANGE_RATE
UNION ALL
SELECT 'DW_SF_FIN_BANK_BRANCH', COUNT(*) FROM DW_FIN_BANK_BRANCH
UNION ALL
SELECT 'DW_SF_FIN_BANK_DEPOSIT', COUNT(*) FROM DW_FIN_BANK_DEPOSIT
UNION ALL
SELECT 'DW_SF_FIN_BANK_RECONCILIATION', COUNT(*) FROM DW_FIN_BANK_RECONCILIATION
UNION ALL
SELECT 'DW_SF_FIN_BANK_RECONCILIAT_DIST', COUNT(*) FROM DW_FIN_BANK_RECONCILIAT_DIST
UNION ALL
SELECT 'DW_SF_FIN_BANK_TRANSACTION', COUNT(*) FROM DW_FIN_BANK_TRANSACTION
UNION ALL
SELECT 'DW_SF_FIN_BANK_TRANSACTION_DETAIL', COUNT(*) FROM DW_FIN_BANK_TRANSACTION_DETAIL
UNION ALL
SELECT 'DW_SF_FIN_BANK_TRANSACTION_TYPE', COUNT(*) FROM DW_FIN_BANK_TRANSACTION_TYPE
UNION ALL
SELECT 'DW_SF_FIN_BANK_TRANS_XCHG_RATE', COUNT(*) FROM DW_FIN_BANK_TRANS_XCHG_RATE
UNION ALL
SELECT 'DW_SF_FIN_BUDGET', COUNT(*) FROM DW_FIN_BUDGET
UNION ALL
SELECT 'DW_SF_FIN_BUDGET_GL_SEGMENT', COUNT(*) FROM DW_FIN_BUDGET_GL_SEGMENT
UNION ALL
SELECT 'DW_SF_FIN_BUDGET_TYPE', COUNT(*) FROM DW_FIN_BUDGET_TYPE
UNION ALL
SELECT 'DW_SF_FIN_CASH_RECEIPT', COUNT(*) FROM DW_FIN_CASH_RECEIPT
UNION ALL
SELECT 'DW_SF_FIN_CASH_RECEIPT_CHARGEBACK', COUNT(*) FROM DW_FIN_CASH_RECEIPT_CHARGEBACK
UNION ALL
SELECT 'DW_SF_FIN_CASH_RECEIPT_DIST', COUNT(*) FROM DW_FIN_CASH_RECEIPT_DIST
UNION ALL
SELECT 'DW_SF_FIN_CASH_RECEIPT_PMT_XFER', COUNT(*) FROM DW_FIN_CASH_RECEIPT_PMT_XFER
UNION ALL
SELECT 'DW_SF_FIN_CASH_RECEIPT_XCHG_RATE', COUNT(*) FROM DW_FIN_CASH_RECEIPT_XCHG_RATE
UNION ALL
SELECT 'DW_SF_FIN_CHART_OF_ACCOUNT', COUNT(*) FROM DW_FIN_CHART_OF_ACCOUNT
UNION ALL
SELECT 'DW_SF_FIN_CLEARING', COUNT(*) FROM DW_FIN_CLEARING
UNION ALL
SELECT 'DW_SF_FIN_CLEARING_DETAIL', COUNT(*) FROM DW_FIN_CLEARING_DETAIL
UNION ALL
SELECT 'DW_SF_FIN_COA_ACCOUNT_GROUP', COUNT(*) FROM DW_FIN_COA_ACCOUNT_GROUP
UNION ALL
SELECT 'DW_SF_FIN_COA_SEGMENT', COUNT(*) FROM DW_FIN_COA_SEGMENT
UNION ALL
SELECT 'DW_SF_FIN_COMMENTS', COUNT(*) FROM DW_FIN_COMMENTS
UNION ALL
SELECT 'DW_SF_FIN_COMPANY', COUNT(*) FROM DW_FIN_COMPANY
UNION ALL
SELECT 'DW_SF_FIN_COMPANY_CURRENCY', COUNT(*) FROM DW_FIN_COMPANY_CURRENCY
UNION ALL
SELECT 'DW_SF_FIN_COMPANY_FISCAL_CALENDAR', COUNT(*) FROM DW_FIN_COMPANY_FISCAL_CALENDAR
UNION ALL
SELECT 'DW_SF_FIN_COMPANY_FISCAL_PERIOD', COUNT(*) FROM DW_FIN_COMPANY_FISCAL_PERIOD
UNION ALL
SELECT 'DW_SF_FIN_COMPANY_FISCAL_YEAR', COUNT(*) FROM DW_FIN_COMPANY_FISCAL_YEAR
UNION ALL
SELECT 'DW_SF_FIN_COMPANY_PARAMETER', COUNT(*) FROM DW_FIN_COMPANY_PARAMETER
UNION ALL
SELECT 'DW_SF_FIN_COMPANY_SEGMENT', COUNT(*) FROM DW_FIN_COMPANY_SEGMENT
UNION ALL
SELECT 'DW_SF_FIN_COMPANY_SEGMENT_VALUE', COUNT(*) FROM DW_FIN_COMPANY_SEGMENT_VALUE
UNION ALL
SELECT 'DW_SF_FIN_CONTACT', COUNT(*) FROM DW_FIN_CONTACT
UNION ALL
SELECT 'DW_SF_FIN_CUSTOMER', COUNT(*) FROM DW_FIN_CUSTOMER
UNION ALL
SELECT 'DW_SF_FIN_CUSTOMER_COMPANY', COUNT(*) FROM DW_FIN_CUSTOMER_COMPANY
UNION ALL
SELECT 'DW_SF_FIN_CUSTOMER_GROUP', COUNT(*) FROM DW_FIN_CUSTOMER_GROUP
UNION ALL
SELECT 'DW_SF_FIN_CUSTOMER_INVOICE', COUNT(*) FROM DW_FIN_CUSTOMER_INVOICE
UNION ALL
SELECT 'DW_SF_FIN_CUST_INVOICE_XCHG_RATE', COUNT(*) FROM DW_FIN_CUST_INVOICE_XCHG_RATE
UNION ALL
SELECT 'DW_SF_FIN_DOCUMENT', COUNT(*) FROM DW_FIN_DOCUMENT
UNION ALL
SELECT 'DW_SF_FIN_DOCUMENT_DETAIL', COUNT(*) FROM DW_FIN_DOCUMENT_DETAIL
UNION ALL
SELECT 'DW_SF_FIN_DOCUMENT_DETAIL_SEGMENT', COUNT(*) FROM DW_FIN_DOCUMENT_DETAIL_SEGMENT
UNION ALL
SELECT 'DW_SF_FIN_DOCUMENT_EXCHANGE_RATE', COUNT(*) FROM DW_FIN_DOCUMENT_EXCHANGE_RATE
UNION ALL
SELECT 'DW_SF_FIN_FISCAL_PERIOD', COUNT(*) FROM DW_FIN_FISCAL_PERIOD
UNION ALL
SELECT 'DW_SF_FIN_FS_CONTROL', COUNT(*) FROM DW_FIN_FS_CONTROL
UNION ALL
SELECT 'DW_SF_FIN_FS_CONTROL_DETAIL', COUNT(*) FROM DW_FIN_FS_CONTROL_DETAIL
UNION ALL
SELECT 'DW_SF_FIN_FS_CONTROL_REPORT', COUNT(*) FROM DW_FIN_FS_CONTROL_REPORT
UNION ALL
SELECT 'DW_SF_FIN_FS_CTL_DETA_EXCLUDE', COUNT(*) FROM DW_FIN_FS_CTL_DETA_EXCLUDE
UNION ALL
SELECT 'DW_SF_FIN_FS_REPORT_SECTION', COUNT(*) FROM DW_FIN_FS_REPORT_SECTION
UNION ALL
SELECT 'DW_SF_FIN_FS_REPORT_SUBSECTION', COUNT(*) FROM DW_FIN_FS_REPORT_SUBSECTION
UNION ALL
SELECT 'DW_SF_FIN_GL_SEGMENT', COUNT(*) FROM DW_FIN_GL_SEGMENT
UNION ALL
SELECT 'DW_SF_FIN_GL_SEGMENT_DETAIL', COUNT(*) FROM DW_FIN_GL_SEGMENT_DETAIL
UNION ALL
SELECT 'DW_SF_FIN_GL_SEGMENT_REPORT', COUNT(*) FROM DW_FIN_GL_SEGMENT_REPORT
UNION ALL
SELECT 'DW_SF_FIN_GL_SEGMENT_SUMMARY_RPT', COUNT(*) FROM DW_FIN_GL_SEGMENT_SUMMARY_RPT
UNION ALL
SELECT 'DW_SF_FIN_INTFS_COMPANY_MAPPING', COUNT(*) FROM DW_FIN_INTFS_COMPANY_MAPPING
UNION ALL
SELECT 'DW_SF_FIN_INVOICE', COUNT(*) FROM DW_FIN_INVOICE
UNION ALL
SELECT 'DW_SF_FIN_INVOICE_DETAIL', COUNT(*) FROM DW_FIN_INVOICE_DETAIL
UNION ALL
SELECT 'DW_SF_FIN_INVOICE_DISTRIBUTION', COUNT(*) FROM DW_FIN_INVOICE_DISTRIBUTION
UNION ALL
SELECT 'DW_SF_FIN_INVOICE_DIST_SEGMENT', COUNT(*) FROM DW_FIN_INVOICE_DIST_SEGMENT
UNION ALL
SELECT 'DW_SF_FIN_INVOICE_EXCHANGE_RATE', COUNT(*) FROM DW_FIN_INVOICE_EXCHANGE_RATE
UNION ALL
SELECT 'DW_SF_FIN_INVOICE_PAY_REQUIREMENT', COUNT(*) FROM DW_FIN_INVOICE_PAY_REQUIREMENT
UNION ALL
SELECT 'DW_SF_FIN_INVOICE_SPLIT_TERM', COUNT(*) FROM DW_FIN_INVOICE_SPLIT_TERM
UNION ALL
SELECT 'DW_SF_FIN_IRO_GL_BOE_DETAILS', COUNT(*) FROM DW_FIN_IRO_GL_BOE_DETAILS
UNION ALL
SELECT 'DW_SF_FIN_IRO_GL_BOE_HEADERS', COUNT(*) FROM DW_FIN_IRO_GL_BOE_HEADERS
UNION ALL
SELECT 'DW_SF_FIN_IRO_PO_CLAIM_DETAILS', COUNT(*) FROM DW_FIN_IRO_PO_CLAIM_DETAILS
UNION ALL
SELECT 'DW_SF_FIN_IRO_PO_CLAIM_HEADERS', COUNT(*) FROM DW_FIN_IRO_PO_CLAIM_HEADERS
UNION ALL
SELECT 'DW_SF_FIN_IRO_PO_DETAILS', COUNT(*) FROM DW_FIN_IRO_PO_DETAILS
UNION ALL
SELECT 'DW_SF_FIN_IRO_PO_HEADERS', COUNT(*) FROM DW_FIN_IRO_PO_HEADERS
UNION ALL
SELECT 'DW_SF_FIN_IRO_PO_RECEIPT_DETAILS', COUNT(*) FROM DW_FIN_IRO_PO_RECEIPT_DETAILS
UNION ALL
SELECT 'DW_SF_FIN_IRO_PO_RECEIPT_HEADERS', COUNT(*) FROM DW_FIN_IRO_PO_RECEIPT_HEADERS
UNION ALL
SELECT 'DW_SF_FIN_ISO_COUNTRY', COUNT(*) FROM DW_FIN_ISO_COUNTRY
UNION ALL
SELECT 'DW_SF_FIN_PAYMENT_METHOD', COUNT(*) FROM DW_FIN_PAYMENT_METHOD
UNION ALL
SELECT 'DW_SF_FIN_PAYMENT_TERM', COUNT(*) FROM DW_FIN_PAYMENT_TERM
UNION ALL
SELECT 'DW_SF_FIN_POSTING_ACCOUNT', COUNT(*) FROM DW_FIN_POSTING_ACCOUNT
UNION ALL
SELECT 'DW_SF_FIN_POSTING_ACT_SEGMENT_DFT', COUNT(*) FROM DW_FIN_POSTING_ACT_SEGMENT_DFT
UNION ALL
SELECT 'DW_SF_FIN_PO_DETAIL', COUNT(*) FROM DW_FIN_PO_DETAIL
UNION ALL
SELECT 'DW_SF_FIN_PURCHASE_ORDER', COUNT(*) FROM DW_FIN_PURCHASE_ORDER
UNION ALL
SELECT 'DW_SF_FIN_REASON', COUNT(*) FROM DW_FIN_REASON
UNION ALL
SELECT 'DW_SF_FIN_RECEIPT', COUNT(*) FROM DW_FIN_RECEIPT
UNION ALL
SELECT 'DW_SF_FIN_RECEIPT_DETAIL', COUNT(*) FROM DW_FIN_RECEIPT_DETAIL
UNION ALL
SELECT 'DW_SF_FIN_SECURITY_ROLE', COUNT(*) FROM DW_FIN_SECURITY_ROLE
UNION ALL
SELECT 'DW_SF_FIN_SECURITY_ROLE_OBJECT', COUNT(*) FROM DW_FIN_SECURITY_ROLE_OBJECT
UNION ALL
SELECT 'DW_SF_FIN_SEGMENT', COUNT(*) FROM DW_FIN_SEGMENT
UNION ALL
SELECT 'DW_SF_FIN_SEGMENT_GROUP_TYPE', COUNT(*) FROM DW_FIN_SEGMENT_GROUP_TYPE
UNION ALL
SELECT 'DW_SF_FIN_SEGMENT_VALUE', COUNT(*) FROM DW_FIN_SEGMENT_VALUE
UNION ALL
SELECT 'DW_SF_FIN_SGMT_GRP_TYPEVAL_SEGVAL', COUNT(*) FROM DW_FIN_SGMT_GRP_TYPEVAL_SEGVAL
UNION ALL
SELECT 'DW_SF_FIN_SGMT_GRP_TYPE_VALUE', COUNT(*) FROM DW_FIN_SGMT_GRP_TYPE_VALUE
UNION ALL
SELECT 'DW_SF_FIN_SPLIT_PAYMENT_TERM', COUNT(*) FROM DW_FIN_SPLIT_PAYMENT_TERM
UNION ALL
SELECT 'DW_SF_FIN_STATE_PROVINCE', COUNT(*) FROM DW_FIN_STATE_PROVINCE
UNION ALL
SELECT 'DW_SF_FIN_TAX', COUNT(*) FROM DW_FIN_TAX
UNION ALL
SELECT 'DW_SF_FIN_UNMATCH_RECEIPT_DETAIL', COUNT(*) FROM DW_FIN_UNMATCH_RECEIPT_DETAIL
UNION ALL
SELECT 'DW_SF_FIN_USER_COMPANY', COUNT(*) FROM DW_FIN_USER_COMPANY
UNION ALL
SELECT 'DW_SF_FIN_USER_COMPANY_DETAIL', COUNT(*) FROM DW_FIN_USER_COMPANY_DETAIL
UNION ALL
SELECT 'DW_SF_FIN_USER_PROFILE', COUNT(*) FROM DW_FIN_USER_PROFILE
UNION ALL
SELECT 'DW_SF_FIN_VENDOR', COUNT(*) FROM DW_FIN_VENDOR
UNION ALL
SELECT 'DW_SF_FIN_VENDOR_COMPANY', COUNT(*) FROM DW_FIN_VENDOR_COMPANY
UNION ALL
SELECT 'DW_SF_FIN_VENDOR_GROUP', COUNT(*) FROM DW_FIN_VENDOR_GROUP
UNION ALL
SELECT 'DW_SF_HIST_FIN_AP_AGING_TRANS', COUNT(*) FROM DW_HIST_FIN_AP_AGING_TRANS
UNION ALL
SELECT 'DW_SF_HIST_FIN_AP_AGING_TRAN_STMT', COUNT(*) FROM DW_HIST_FIN_AP_AGING_TRAN_STMT
UNION ALL
SELECT 'DW_SF_FIN_BUDGET_GL_SEGMENT_DET', COUNT(*) FROM DW_FIN_BUDGET_GL_SEGMENT_DET
UNION ALL
SELECT 'DW_SF_FIN_IFI_AP_INVOICE', COUNT(*) FROM DW_FIN_IFI_AP_INVOICE
'''