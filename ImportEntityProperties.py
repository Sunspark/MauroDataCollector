import argparse
import textwrap
import logging
import datetime
import csv

#####################################################################
## Argument Parsing
#####################################################################

help_text = "Imports entity properties to Mauro Data Mapper. Requires a user to have set up an API key."
epilog_text = '''Incoming file format must be CSV. The first row of the file must be headers. Order of the columns doesn't matter. 
The file must contain columns 'db','schema','table','field' if these are not specified by command line options.
If the file contains the column 'description', this will be added to the 'description' of the entity. Any other column headers will be added as properties named as the column headers.
Column headers will be treated as dot separated string lists. The last element of the list will be treated as the property name, with the rest of the list being treated as the namespace of the property. If there is no namespace in the column header, the namespace must be specified in the command line options.
'''

ap = argparse.ArgumentParser(
  description=help_text,
  epilog=textwrap.dedent(epilog_text)
)
ap.add_argument('-l',
  '--log-level',
  action='store',
  nargs=1, # '?' is optional 1
  default='INFO',
  choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
  help='Sets File logging to specified level (default INFO). Overridden by --verbose.'
)
ap.add_argument('-v',
  '--verbose',
  action='store_true',
  help='Sets File logging to DEBUG (default info), and StdOut logging to INFO (default none). Overrides --log-level.'
)
ap.add_argument('-p',
  '--log-path',
  action='store',
  default='logs/',
  help="Sets destination path for file logs. Default 'logs/'."
)

# -w --delete-word Sets a string that causes the property value to be 'deleted' (set to null). Default '##delete##'.
# -n --delete-null Sets processing to to set property values to null (delete them) when a null value is encountered. Default behaviour is to ignore incoming null values (retain any existing values, and not add null-value properties).
# -b --always-branch Sets processing to always create a new branch of an entity when importing a file. Default behaviour is to Update current branch if 'draft', create a new branch if 'finalised'.
# db/schema/table/field Sets override value for x when processing. Values in the incoming file will be ignored. Allows not including the value in the incoming file.
# -i --incoming-file Sets filename of incoming file to be processed. File format described below.
# -j --incoming-dir Sets path to directory of incoming files (default ./). Processing will attempt to process all files in the directory that match (*.csv).
# https://docs.python.org/3/library/argparse.html#mutual-exclusion
# -u --mauro-url Sets URL of the Mauro API
# -k --mauro-api-key Sets API key for interacting with the Mauro API
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
## Actual program
#####################################################################

logger.info('Execution begins')



print("fish")




logger.info('Execution ends')