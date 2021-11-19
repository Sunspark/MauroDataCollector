import argparse
import textwrap
import logging
import datetime
import os
import sys

from pathlib import Path
import pandas as pd


#####################################################################
## Argument Parsing
#####################################################################

help_text = "Reformats an .xlsx file into a 'Simple Excel Model' file, suitable for import to Mauro"
epilog_text = '''Incoming file format must be .xlsx, with a sheet named "Data Specifications", containing columns 'ServerName', 'DatabaseName', 'schemaName', 'TableName', 'TableDesc', 'ColumnName', 'ColumnDesc'.
Currently, the data types of all incoming fields are set to 'Unknown'
'''
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
  '-i',
  '--incoming-file',
  action='store',
  required=True,
  help="Sets filename of incoming file to be processed. File format described below."
)
ap.add_argument(
  '-o',
  '--output-file',
  action='store',
  default='',
  help="Sets filename of output file to be produced. Default '<incoming_file_name>_ReformedForMauro_<timestamp>.xlsx'"
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
f_handler = logging.FileHandler(args.log_path + 'SheetReformatter_' + now_string + '.log', encoding='utf-8')
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

incoming_file_path = str(args.incoming_file)
logger.debug('Requested file: ' + incoming_file_path)
incoming_file_name = Path(incoming_file_path).name.split('.')[0]
logger.debug('incoming_file_name: ' + incoming_file_name)

output_file_path = args.output_file
if output_file_path == '':
  output_file_path = incoming_file_name + '_ReformedForMauro_' + now_string + '.xlsx'

logger.debug('output_file_path: ' + output_file_path)


# https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html
# specify sheet_name to read a named sheet, don't to get all sheets
required_cols = [
  'ServerName',
  'DatabaseName',
  'schemaName',
  'TableName',
  'TableDesc',
  'ColumnName',
  'ColumnDesc'
]
logger.info("Attempting to read file:" + incoming_file_path)
try:
  incoming_sheet_frame = pd.read_excel(incoming_file_path, sheet_name="Data Specifications", usecols=required_cols)
except ValueError as err:
  err_and_die(err, "Incoming file read failed")

logger.debug('Read OK, processing')
incoming_sheet_frame.replace(to_replace='None', value='', inplace=True)

# Check that the fields we need are present
if not set(required_cols).issubset(incoming_sheet_frame.columns):
  crit_and_die("Required columns not found in incoming dataframe")

# First , create paths to all the things.
incoming_sheet_frame['PathToDbSchema'] = incoming_sheet_frame.apply (lambda x: str(x['DatabaseName']) + ' | ' + str(x['schemaName']), axis=1)
incoming_sheet_frame['PathToDbSchemaTable'] = incoming_sheet_frame.apply (lambda x: str(x['PathToDbSchema']) + ' | ' + str(x['TableName']), axis=1)

# open 'output stream'
logger.debug("Attempting to open output file:" + output_file_path)
writer = pd.ExcelWriter(output_file_path, engine='xlsxwriter')

# Create Asset Model sheet
logger.debug('Create Asset Model sheet')
server_names = incoming_sheet_frame['ServerName'].unique()
data_model_frame = pd.DataFrame({
  'Name' : server_names,
  'Description':None,
  'Author':None,
  'Organisation':None,
  'Sheet Key' : server_names,
  'Type': 'Data Asset'
})
data_model_frame.to_excel(writer, sheet_name='DataModels', index=False)

# Fake Enumerations
logger.debug('Fake Enumerations')
enumerations_frame = pd.DataFrame(columns=[
  'DataModel Name',
  'Enumeration Name',
  'Description',
  'Key',
  'Value'
])
enumerations_frame.to_excel(writer, sheet_name='Enumerations', index=False)


def process_sheet_for_server(server_name):
  logger.info('----- Processing server name:' + server_name)

  # get incoming server name, and pull all rows for that.
  this_server_df = incoming_sheet_frame[incoming_sheet_frame['ServerName'] == server_name]

  # Column names in the right order for output
  element_cols = [
    'DataClass Path',
    'DataElement Name',
    'Description',
    'Minimum Multiplicity',
    'Maximum Multiplicity',
    'DataType Name',
    'DataType Reference'
  ]

  # DB names
  logger.debug('Processing DB names')
  db_unique_names = pd.DataFrame(this_server_df['DatabaseName'].unique(), columns=['DataClass Path'])
  db_unique_names = db_unique_names.join(pd.DataFrame(
      {
        'DataElement Name':'',
        'Description':'',
        'Minimum Multiplicity':'1',
        'Maximum Multiplicity':'1',
        'DataType Name':'',
        'DataType Reference':''
      }, index=db_unique_names.index
  ))
  db_unique_names = db_unique_names[element_cols] # Reorder the frame to be the right order for output

  # Schema names
  logger.debug('Processing schema names')
  schema_unique_frame = this_server_df.drop_duplicates(subset = ["PathToDbSchema"])
  schema_unique_frame = schema_unique_frame[['PathToDbSchema']]
  schema_unique_frame.rename(columns={'PathToDbSchema':'DataClass Path'}, inplace=True)
  schema_unique_frame = schema_unique_frame.join(pd.DataFrame(
      {
        'DataElement Name':'',
        'Description':'',
        'Minimum Multiplicity':'1',
        'Maximum Multiplicity':'1',
        'DataType Name':'',
        'DataType Reference':''
      }, index=schema_unique_frame.index
  ))
  schema_unique_frame = schema_unique_frame[element_cols] # Reorder the frame to be the right order for output

  # Table names
  logger.debug('Processing Table names')
  table_unique_frame = this_server_df.drop_duplicates(subset = ["PathToDbSchemaTable"])
  table_unique_frame = table_unique_frame[['TableDesc', 'PathToDbSchemaTable']]
  table_unique_frame.rename(columns={'PathToDbSchemaTable':'DataClass Path', 'TableDesc':'Description'}, inplace=True)
  table_unique_frame = table_unique_frame.join(pd.DataFrame(
      {
        'DataElement Name':'',
        'Minimum Multiplicity':'1',
        'Maximum Multiplicity':'1',
        'DataType Name':'',
        'DataType Reference':''
      }, index=table_unique_frame.index
  ))
  table_unique_frame = table_unique_frame[element_cols] # Reorder the frame to be the right order for output

  # Fields, the actual Elements!
  logger.debug('Processing Field names')
  fields_frame = this_server_df[['ColumnName', 'ColumnDesc', 'PathToDbSchemaTable']]
  fields_frame = fields_frame.rename(columns={'PathToDbSchemaTable':'DataClass Path', 'ColumnDesc':'Description', 'ColumnName':'DataElement Name'}) # create new df to preserve this_server_df during rename
  fields_frame = fields_frame.join(pd.DataFrame(
      {
        'Minimum Multiplicity':'1',
        'Maximum Multiplicity':'1',
        'DataType Name':'Unknown',
        'DataType Reference':''
      }, index=fields_frame.index
  ))
  fields_frame = fields_frame[element_cols] # Reorder the frame to be the right order for output

  # Stitch it for output
  logger.debug('Stitching for output')
  final_frame = pd.concat([db_unique_names, schema_unique_frame, table_unique_frame, fields_frame], ignore_index=True)
  #final_frame.sort_values(by=['DataClass Path', 'DataElement Name'], inplace=True)

  final_frame.to_excel(writer, sheet_name=server_name, index=False)
  logger.debug('----- End of loop')


for server_name in server_names:
  process_sheet_for_server(server_name) 


logger.info('Closing out output file')
writer.save()

logger.info('Execution Ends')