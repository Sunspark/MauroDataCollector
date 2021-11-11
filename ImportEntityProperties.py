import argparse
import logging
import datetime
import csv

#####################################################################
## Argument Parsing
#####################################################################

ap = argparse.ArgumentParser()
ap.add_argument('-v',
  '--verbose',
  action='store_true',
  help='Sets File logging to DEBUG (default info), and StdOut logging to INFO (default none)'
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
f_handler = logging.FileHandler('logs/' + 'ImportEntityProperties_' + now_string + '.log', encoding='utf-8')
file_logging_level = logging.WARNING
if args.verbose:
  file_logging_level = logging.DEBUG
f_handler.setLevel(file_logging_level)

# Create formatters and add it to handlers
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
f_handler.setFormatter(logging.Formatter(log_format))

# Add handlers to the logger
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