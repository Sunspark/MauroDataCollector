import argparse
import textwrap
import logging
import datetime
import csv
import json
#import requests
import os
import re
import sys

from MauroAPIInterface import MauroAPIInterface

#####################################################################
## Argument Parsing
#####################################################################

help_text = "Imports entity properties to Mauro Data Mapper. Requires a user to have set up an API key."
epilog_text = '''Incoming file format must be CSV. The first row of the file must be headers (case sensitive). Order of the columns doesn't matter. 
The file must contain columns 'db','table','field' if these are not specified by command line options. 'schema' is optional, and is the tier between 'db' and 'table' if present.
If the file contains the column 'description', this will be added to the 'description' of the entity. Any other column headers will be added as properties named as the column headers.
Column headers will be treated as dot separated string lists. The last element of the list will be treated as the property name, with the rest of the list being treated as the namespace of the property. If there is no namespace in the column header, the namespace must be specified in the command line options.
Processing will treat a passed value of the string 'null' or a blank string '' in the CSV as NULL.
Column headers must be unique.
'''

# 'db' corresponds to 'Data Asset' at the 'Data Model' level in the UI. An unknown db will be created as a Data Asset. db name must be unique across all folders in the Mauro store.
# 'schema' and 'table' correspond to 'Data Class' in the UI. These are hierachical, and must have unique names within the parent. If no schema is specified (null), the table will be created as a child of db.
# 'field' corresponds to 'Data Element' in the UI. These must be unique within the parent class.
# Will attempt to find the entity by name, and will pick the latest draft branch, or finalised if no drafts

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
# db/schema/table/field Sets override value for x when processing. Values in the incoming file will be ignored. Allows not including the value in the incoming file.
ap.add_argument(
  '-d',
  '--db',
  action='store',
  help="Sets override value for db when processing. Values in the incoming file will be ignored. Allows not including the value in the incoming file."
)
ap.add_argument(
  '-s',
  '--schema',
  action='store',
  help="Sets override value for schema when processing. Values in the incoming file will be ignored. Allows not including the value in the incoming file."
)
ap.add_argument(
  '-t',
  '--table',
  action='store',
  help="Sets override value for table when processing. Values in the incoming file will be ignored. Allows not including the value in the incoming file."
)
ap.add_argument(
  '-f',
  '--field',
  action='store',
  help="Sets override value for field when processing. Values in the incoming file will be ignored. Allows not including the value in the incoming file."
)
# https://docs.python.org/3/library/argparse.html#mutual-exclusion
fileGroup = ap.add_mutually_exclusive_group(required = True)
fileGroup.add_argument(
  '-j',
  '--incoming-dir',
  action='store',
  default='',
  help="Sets path to directory of incoming files (default ./). Processing will attempt to process all files in the directory that match (*.csv)."
)
fileGroup.add_argument(
  '-i',
  '--incoming-file',
  action='store',
  help="Sets filename of incoming file to be processed. File format described below."
)

ap.add_argument(
  '-u',
  '--mauro-url',
  action='store',
  required=True,
  help="Sets URL of the Mauro API. e.g.'http://localhost:8082/api'"
)
ap.add_argument(
  '-k',
  '--mauro-api-key',
  action='store',
  required=True,
  help="Sets API key for interacting with the Mauro API. Note that the API won't return 'bad key' - it only returns 404."
)

# -w --delete-word Sets a string that causes the property value to be 'deleted' (set to null). Default '##delete##'.
# -n --delete-null Sets processing to to set property values to null when a null value is encountered. Default behaviour is to ignore incoming null values (retain any existing values, and not add null-value properties).
# -b --always-branch Sets processing to always create a new branch of an entity when importing a file. Default behaviour is to Update current branch if 'draft', create a new branch if 'finalised'.

# -x --target-folder Sets target folder to process the incoming model data to.
# -p --namespace Sets a default namespace for the incoming property keys. If you need to import to different namespaces, you can set the namespace per property in the incoming file. https://maurodatamapper.github.io/rest-api/resources/catalogue-item/#metadata
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
f_handler = logging.FileHandler(args.log_path + 'ImportEntityProperties_' + now_string + '.log', encoding='utf-8')
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
## Helper functions
#####################################################################

def build_files_to_process():
  logger.debug("build_files_to_process")
  files_to_process = []
  for root, subFolders, files in os.walk(args.incoming_dir):
    logger.debug("root : " + str(root))
    logger.debug("subFolders : " + str(subFolders))
    logger.debug("files : " + str(files))
    for filename in files:
      logger.debug("current filename : " + str(filename))
      if (filename.endswith(".csv")):
        logger.debug("added!")
        files_to_process.append(filename)
    break # break here to not traverse the directory tree
  return files_to_process

# https://stackoverflow.com/questions/9835762/how-do-i-find-the-duplicates-in-a-list-and-create-another-list-with-them
def list_duplicates(list_to_parse):
  logger.debug('list_duplicates')
  logger.debug(list_to_parse)
  seen = list()
  # adds all elements it doesn't know yet to seen and all other to seen_twice
  seen_twice = list( x for x in list_to_parse if x in seen or seen.append(x) )
  logger.debug(seen_twice)
  return seen_twice

def make_null(v):
  if ((v.upper() == 'NULL') or (v == '')):
    return None
  else:
    return v


#####################################################################
## Actual program
#####################################################################

logger.info('Execution begins')
print("fish")


logger.debug('Requested file: ' + str(args.incoming_file))
logger.debug('Requested directory: ' + args.incoming_dir)


# Get files to process
files_to_process = []
if (args.incoming_file):
  files_to_process.append(args.incoming_file)
else:
  files_to_process = build_files_to_process()

logger.info('Files to process count: ' + str(len(files_to_process)))
logger.debug('Files to process:')
logger.debug(files_to_process)


for target_filename in files_to_process:

  # Get the CSV, and spin it to a list of lists.
  target_fullpath = os.path.join(args.incoming_dir, target_filename)

  logger.info("Attempting to open: '" + target_fullpath + "'")

  try:
    with open(target_fullpath) as csv_in:
      incoming_rows = [list(line) for line in csv.reader(csv_in)]

  except FileNotFoundError as err:
    err_and_die(err, 'File Not Found: ' + target_fullpath)


  headers = incoming_rows.pop(0) # Take the header row off
  logger.info("Count of incoming rows: " + str(len(incoming_rows)))

  logger.debug("headers:")
  logger.info(headers)
  headers_count = len(headers)
  logger.info("Count of incoming headers: " + str(headers_count))

  # Check we have unique headers
  duplicate_headers = list_duplicates(headers)
  logger.debug("Count of duplicate headers: " + str(len(duplicate_headers)))
  if (len(duplicate_headers) > 0) :
    logger.error("Duplicate vaules:")
    logger.error(duplicate_headers)
    crit_and_die('Duplicate vaules found in incoming file header row. Consider namespacing.')


  # Check we have pre-req fields in the CSV (or args)
  # https://stackoverflow.com/questions/7571635/fastest-way-to-check-if-a-value-exists-in-a-list

  if not (('db' in headers) or (args.db)):
    crit_and_die("Essential header 'db' not found in incoming file.")

  if not (('table' in headers) or (args.table)):
    crit_and_die("Essential header 'table' not found in incoming file.")

  if not (('field' in headers) or (args.field)):
    crit_and_die("Essential header 'field' not found in incoming file.")

  logger.debug('Essential headers ok.')

  # Create override dictionary for rows
  override_dict = {}
  if args.db is not None:
    logger.debug("Overriding db to '"+ str(args.db) + "'")
    override_dict['db'] = args.db

  if args.schema is not None:
    logger.debug("Overriding schema to '"+ str(args.schema) + "'")
    override_dict['schema'] = args.schema

  if args.table is not None:
    logger.debug("Overriding table to '"+ str(args.table) + "'")
    override_dict['table'] = args.table

  if args.field is not None:
    logger.debug("Overriding field to '"+ str(args.field) + "'")
    override_dict['field'] = args.field

  logger.debug('Override dictionary created:')
  logger.debug(override_dict)    

  # Create interface object for throwing things at the API
  api_base_url = args.mauro_url
  logger.info("Connecting to Mauro API at: " + str(api_base_url))
  try:
    mapi = MauroAPIInterface(logger, api_base_url)
  except ValueError as err :
    err_and_die(err, "Could not create Mauro API interface")

  api_key = args.mauro_api_key
  logger.debug("Incoming API key not logged, as it's secret.")
  try:
    mapi.api_key = api_key
  except ValueError as err :
    err_and_die(err, "Given API key appears to be bad. It should look like a UUID.")

  # Process incoming rows
  for incoming_row in incoming_rows:
    logger.debug('Process incoming row:')
    logger.debug(incoming_row)

    # Check field count is the same as headers.
    row_field_count = len(incoming_row)
    logger.debug('Row item count: ' + str(row_field_count))
    if (headers_count != row_field_count) : # Potential additional functionality here to not crit and die.
      crit_and_die("Row item count did not match header count.")

    row_dict = dict(zip(headers, incoming_row))
    logger.debug("Pre-nulling row dictionary:")
    logger.debug(row_dict)

    row_dict = dict((k, make_null(v)) for k, v in row_dict.items())
    logger.debug("Post-nulling row dictionary:")
    logger.debug(row_dict)

    # Apply Command-line overrides
    row_dict = row_dict | override_dict
    logger.debug("Post-override row dictionary:")
    logger.debug(row_dict)

    logger.debug("Attempting to find ID-based URL for:")
    logger.debug("  'db': " + str(row_dict['db']))
    logger.debug("  'schema': " + str(row_dict['schema']))
    logger.debug("  'table': " + str(row_dict['table']))
    logger.debug("  'field': " + str(row_dict['field']))

    path_string = ''
    if row_dict['db'] is not None :
      path_string = path_string + "dm:" + str(row_dict['db'])
      if row_dict['schema'] is not None :
        path_string = path_string + "|dc:" + str(row_dict['schema'])
      if row_dict['table'] is not None :
        path_string = path_string + "|dc:" + str(row_dict['table'])
      if row_dict['field'] is not None :
        path_string = path_string + "|de:" + str(row_dict['field'])
    else :
      crit_and_die("Essential variable 'db' not provided.")
    
    logger.debug("Constructed path: " + path_string)

    # Returns a dictionary :
    #   'status_code' : The numeric status of the http response
    #   'url_found' : True || False - was an appropriate URL found. Note that the API can return paths that don't match the incoming path.
    #   'model_finalised' : True || False - Is the current head of the data model 'finalised'.
    #   'id_based_url' : A string with the ID-based path to the entitiy
    #   'r' : the requests library object, so that it can be bubbled up or interrogated
    search_dict = mapi.find_id_based_url_by_path(path_string)

    logger.debug(search_dict)

    
    if search_dict['status_code'] == 404 :
      logger.error("Entity not found in Mauro. Entity update has been skipped.")
      logger.error("Constructed path: " + path_string)
      logger.error(search_dict)
    elif search_dict['status_code'] == 200 :
      logger.debug("Entity lookup code 200 OK.")

      if search_dict['url_found'] is True :
        logger.debug("Entity lookup code 200 OK, url_found:True")

        if search_dict['model_finalised'] is False :
          logger.debug("Entity lookup code 200 OK, url_found:True, model_finalised:False")
    #200 good draft
        else :
          logger.debug("Entity lookup code 200 OK, url_found:True, model_finalised:True")
    #200 good finalised

      else :
        logger.error("Entity lookup succeeded, but entity was bad. Entity update has been skipped.")
        logger.error("Constructed path: " + path_string)
        logger.error(search_dict)



    else :
      logger.error("Entity lookup failed for some reason. Entity update has been skipped.")
      logger.error("Constructed path: " + path_string)
      logger.error(search_dict)


    # goes to the API, and figures out the ID-based URL to target, as well as POST/ PUT.
    # Returns dict {'target_url': target_url , 'http_method': (POST | PUT)}
    # Note the http_method will never be DELETE, because 'delete' means null the value, not remove the entity.
    # Removing the entity needs a different interface
    #def get_api_target(db, schema, table, field):
    #  return 1



    # Going to need to handle OK and not OK, as well as really not OK.
    # as well as checking the result to see if the API was lying....










logger.info('Execution ends')


