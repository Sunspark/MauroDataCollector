import argparse
import textwrap
import logging
import datetime
import os
import sys

from pathlib import Path
import pyodbc
import pandas as pd


#####################################################################
## Argument Parsing
#####################################################################

help_text = "Connects to a specified MSSQL database (including Azure instances), and outputs a 'Simple Excel Model' file, suitable for import to Mauro"
epilog_text = ''

ap = argparse.ArgumentParser(
  description=help_text,
  epilog=textwrap.dedent(epilog_text)
)
ap.add_argument(
  '-l',
  '--log-level',
  action='store',
  nargs=1, # '?' is optional 1
  default='INFO',
  choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
  help='Sets File logging to specified level (default INFO). Overridden by --verbose.'
)
ap.add_argument(
  '-v',
  '--verbose',
  action='store_true',
  help='Sets File logging to DEBUG (default info), and StdOut logging to INFO (default none). Overrides --log-level.'
)
ap.add_argument(
  '-p',
  '--log-path',
  action='store',
  default='logs/',
  help="Sets destination path for file logs. Default 'logs/'."
)
ap.add_argument(
  '-s',
  '--server',
  action='store',
  required=True,
  help="Server to connect to. Can be an instance path in the form <server_name>\<instance_name>. This will be reformatted in the output to <server_name>|<instance_name>."
)
ap.add_argument(
  '-d',
  '--db',
  action='store',
  required=True,
  help="Database to connect to. Currently, this script only does one DB at a time."
)
ap.add_argument(
  '-u',
  '--username',
  action='store',
  required=True,
  help="Username for connection"
)
ap.add_argument(
  '-w',
  '--password',
  action='store',
  required=True,
  help="Password for connection"
)
ap.add_argument(
  '-o',
  '--output-file',
  action='store',
  default='',
  help="Sets filename of output file to be produced. Default '<server_name>_<database_name>_ForMauro_<timestamp>.xlsx'"
)
args = ap.parse_args()

#####################################################################
## Log Handling
#####################################################################

# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) #Sets the level that is passed to the handlers. The handlers then determine what is passed onward.

# Create handlers
now_string = datetime.datetime.now().strftime("%Y%m%d-%H%M%S%f")
if not args.log_path.endswith('/'):
  args.log_path = args.log_path + '/'
f_handler = logging.FileHandler(args.log_path + 'GetMSSQLSchemaToExcel_' + now_string + '.log', encoding='utf-8')
strloglevel = args.log_level
intloglevel = getattr(logging, strloglevel.upper())
file_logging_level = intloglevel
if args.verbose:
  file_logging_level = logging.DEBUG
f_handler.setLevel(file_logging_level)

# Create formatters and add it to handlers
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
f_handler.setFormatter(logging.Formatter(log_format))

# Add handlers to the logger. Nothing logs prior to now.
logger.addHandler(f_handler)
if args.verbose:
  c_handler = logging.StreamHandler()
  c_handler.setLevel(logging.INFO)
  c_handler.setFormatter(logging.Formatter(log_format))
  logger.addHandler(c_handler)

#####################################################################
## Handle exiting
#####################################################################

def crit_and_die(message):
  logger.critical(message)
  logger.critical('Execution ends')
  sys.exit(message)

def err_and_die(err, message):
  logger.exception(err)
  logger.critical(message)
  logger.critical('Execution ends')
  sys.exit(err)

#####################################################################
## Actual program
#####################################################################

logger.info('Execution begins')

target_server_name = str(args.server)
logger.debug('target_server_name: ' + target_server_name)
target_server_name_output = target_server_name.replace('\\','_') # Windows can't have slashes or pipes
logger.debug('target_server_name_output: ' + target_server_name_output)
target_server_name_excel = target_server_name.replace('\\','|') # Excel sheets can't have backslashes in the name
logger.debug('target_server_name_excel: ' + target_server_name_excel)
target_db_name = str(args.db)
logger.debug('target_db_name: ' + target_db_name)
target_user_name = str(args.username)
logger.debug('target_user_name: ' + target_user_name)
target_password = str(args.password)
logger.debug('target_password: [Redacted]')

output_file_path = args.output_file
if output_file_path == '':
  output_file_path = target_server_name_output + '_' + target_db_name + '_ForMauro_' + now_string + '.xlsx'

logger.debug('output_file_path: ' + output_file_path)

logger.info('Attempting server connection')
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+target_server_name+';DATABASE='+target_db_name+';UID='+target_user_name+';PWD='+ target_password)


# Script doesn't do multiple databases right now
#DatabasesQuery = 'SELECT name FROM master.sys.databases'

logger.info('Attempting to collect tables')
TablesQuery = '''
SELECT
  CONCAT(
    t.TABLE_CATALOG
    , ' | '
    , t.TABLE_SCHEMA
    , ' | '
    , t.TABLE_NAME
  ) AS 'DataClass Path'
  /*
  , t.TABLE_CATALOG
  , t.TABLE_SCHEMA
  , t.TABLE_NAME
  */
  , t.TABLE_TYPE AS 'database.mssql.tableinfo.table_type'
  , ISNULL(CONVERT(VARCHAR(2000), d.[value]), '') AS 'Description'
FROM
  INFORMATION_SCHEMA.TABLES t
  OUTER APPLY fn_listextendedproperty ('MS_Description', 'schema', t.TABLE_SCHEMA, 'table', t.TABLE_NAME, NULL, NULL) d
WHERE
  t.TABLE_TYPE IN ('BASE TABLE', 'VIEW')  
'''

tables_frame = pd.read_sql_query(TablesQuery, cnxn)
logger.info('Count of tables returned: ' + str(len(tables_frame)))

logger.info('Attempting to collect columns')
ColumnsQuery = '''
SELECT
  CONCAT(
    t.TABLE_CATALOG
    , ' | '
    , t.TABLE_SCHEMA
    , ' | '
    , t.TABLE_NAME
  ) AS 'DataClass Path'
  /*
  , t.TABLE_CATALOG
  , c.TABLE_SCHEMA
  , c.TABLE_NAME
  */
  , c.COLUMN_NAME AS 'DataElement Name'
  , ISNULL(CONVERT(VARCHAR(2000), d.[value]), '') AS 'Description'
  , c.ORDINAL_POSITION AS 'database.mssql.columninfo.ordinal_position'
  , c.IS_NULLABLE AS 'database.mssql.columninfo.is_nullable'
  , UPPER(c.DATA_TYPE) AS 'DataType Name'
  , c.CHARACTER_MAXIMUM_LENGTH AS 'database.mssql.columninfo.character_maximum_length'
  , c.CHARACTER_OCTET_LENGTH AS 'database.mssql.columninfo.character_octet_length'
  , c.NUMERIC_PRECISION AS 'database.mssql.columninfo.numeric_precision'
  , c.NUMERIC_SCALE AS 'database.mssql.columninfo.numeric_scale'
  , c.DATETIME_PRECISION AS 'database.mssql.columninfo.datetime_precision'
  , ISNULL(c.COLUMN_DEFAULT, '') AS 'database.mssql.columninfo.column_default'
FROM
  INFORMATION_SCHEMA.TABLES t
  LEFT JOIN INFORMATION_SCHEMA.COLUMNS c ON (
    t.TABLE_SCHEMA = c.TABLE_SCHEMA
    AND t.TABLE_NAME = c.TABLE_NAME
  )
  OUTER APPLY fn_listextendedproperty ('MS_Description', 'schema', t.TABLE_SCHEMA, 'table', t.TABLE_NAME, 'column', c.COLUMN_NAME) d
WHERE
  t.TABLE_TYPE IN ('BASE TABLE', 'VIEW')
'''

columns_frame = pd.read_sql_query(ColumnsQuery, cnxn)
logger.info('Count of tables returned: ' + str(len(columns_frame)))

logger.info('Reformatting for output')
joined_frame  = pd.concat([tables_frame, columns_frame]).drop_duplicates().reset_index(drop=True)
joined_frame = joined_frame.join(pd.DataFrame(
    {
      'Minimum Multiplicity':'1',
      'Maximum Multiplicity':'1',
      'DataType Reference':''
    }, index=joined_frame.index
))
joined_frame.fillna('', inplace=True) # Get rid of NaN
int_cols = [ # Pandas helpfully casts all these ints to floats, because ints can't be NaN
  'database.mssql.columninfo.ordinal_position',
  'database.mssql.columninfo.character_maximum_length',
  'database.mssql.columninfo.character_octet_length',
  'database.mssql.columninfo.numeric_precision',
  'database.mssql.columninfo.numeric_scale',
  'database.mssql.columninfo.datetime_precision'
]
for col in int_cols:
   joined_frame[col] = joined_frame[col].apply(lambda x: int(x) if x != '' else x) # Convert all the ints back to ints


logger.info('Attempting to open output file')
writer = pd.ExcelWriter(output_file_path, engine='xlsxwriter')

logger.debug('Writing server')
data_model_frame = pd.DataFrame({
  'Name' : target_server_name_excel,
  'Description':None,
  'Author':None,
  'Organisation':None,
  'Sheet Key' : target_server_name_excel,
  'Type': 'Data Asset'
},index=['0'])
data_model_frame.to_excel(writer, sheet_name='DataModels', index=False)

logger.debug('Writing enumerations')
enumerations_frame = pd.DataFrame(columns=[
  'DataModel Name',
  'Enumeration Name',
  'Description',
  'Key',
  'Value'
])
enumerations_frame.to_excel(writer, sheet_name='Enumerations', index=False)

logger.debug('Writing tables, views, columns')
joined_frame.to_excel(writer, sheet_name=target_server_name_excel, index=False)

logger.info('Closing out output file')
writer.save()

logger.info('Execution Ends')