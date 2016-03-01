#!/usr/bin/env python2.7
from Genesis.GenesisClientTaskTracker import *

#Parse parameters 
import argparse

logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)
logger.propagate=True

ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(levelname)s - %(asctime)s - %(name)s: %(message)s   '))

logger.addHandler(ch)

def get_input_parameters():
	parser = argparse.ArgumentParser(description='Task tracker')
	parser.add_argument('-c', '--configuration-file',  dest='config',  required=False, action='store', help='Full path to configuration file')
	parser.add_argument('-t', '--task-id',  dest='task_id',  required=True, action='store', help='Task Id')

	parser.add_argument('-l', '--log-level',  dest='log_level',  required=False, action='store', help='Log level')

	parser.add_argument('-V', '--version', action='version', version="%(prog)s version 0.0.1")

	return vars(parser.parse_args())


if __name__ == "__main__":

        execution_folder = os.path.dirname(os.path.abspath(__file__))


	args = get_input_parameters()


        if args['config']:
                config = args['config']
        else:
                config = '{0}/.genesis-config'.format(execution_folder)

        if args['log_level']:

                if args['log_level'].lower() == 'debug':
                        log_level = logging.DEBUG
                if args['log_level'].lower() == 'info':
                        log_level = logging.INFO
                if args['log_level'].lower() == 'warning':
                        log_level = logging.WARNING
                if args['log_level'].lower() == 'critical':
                        log_level = logging.CRITICAL
                if args['log_level'].lower() == 'error':
                        log_level = logging.ERROR


        else:
                log_level = logging.ERROR


	logger = logging.getLogger(__name__)
	logger.setLevel(log_level)	
	
	tracker = GenesisClientTaskTracker( args['task_id'], args['config'], log_level=log_level)
	
	tracker.run()

	sys.exit(0)
