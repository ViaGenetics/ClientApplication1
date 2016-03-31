#!/usr/bin/env python
import subprocess, sys, logging, os
from Genesis.GenesisClientCoordinator import GenesisClientCoordinator as GenesisClientCoordinator 

#Parse parameters 
import argparse


logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)
logger.propagate=True

ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(levelname)s - %(asctime)s - %(name)s: %(message)s   '))

logger.addHandler(ch)

def get_input_parameters():
	parser = argparse.ArgumentParser(description='Genesis Client Agent')

	parser.add_argument('-c', '--configuration-file',  dest='config',  required=False, action='store', help='Full path to configuration file')
	parser.add_argument('-V', '--version', action='version', version="%(prog)s version 0.0.1")

	parser.add_argument('-l', '--log-level',  dest='log_level',  required=False, action='store', help='Log level')

	parser.add_argument('--start',  action='store_true',  required=False,   help='Start daemon')
	parser.add_argument('--stop',  action='store_true',  required=False,   help='Stop daemon')
	parser.add_argument('--restart',  action='store_true',  required=False,   help='Restart daemon')
	parser.add_argument('--debug',  action='store_true',  required=False,     help='Debug mode run one iteration only, print to stdout')
	parser.add_argument('--verify',  action='store_true',  required=False,   help='Tests installation')
	parser.add_argument('--configure',  action='store_true',  required=False,   help='Create configuration file')


	parser.add_argument('--log',  action='store_true',  required=False,     help='Will print Daemon stdout and stderr into files inside of logs folder')

	return vars(parser.parse_args())



def test_install(execution_folder):
	missing = []
	try:
		import requests
	except:
		missing.append('requests')

	MacOSX_folder = ''
	if sys.platform == "darwin":
		MacOSX_folder = "MacOSX/"

	proc = subprocess.Popen(['{0}/bin/{1}ua'.format(execution_folder, MacOSX_folder), '-h'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	out, err = proc.communicate()

	if proc.returncode != 0:
		missing.append('Unable to execute Upload Agent')

	return missing


def verify_configuration_file(execution_folder):
	import Genesis.GenesisUtils as helper
	try:
		return helper.load_configuration('{0}/.genesis-config'.format(execution_folder))
	except:
		return None


def create_config(config_file):
	client_id = str(raw_input("Provide Client Id:"))
	secret_key = str(raw_input("Provide Secret key:"))

	report_issues = str(raw_input("Report errors? [1]:"))
	if report_issues.strip() == '':
		report_issues = 1
	else:
		report_issues = int(report_issues)



	if client_id.strip() == '' or secret_key.strip() == '':
		sys.stderr.write('Client Id and Secret Key are mandatory, no values were saved\n')
		return False
	else:
		content = []
		if os.path.exists(config_file):
			with open(config_file, 'r') as file:
				content = file.readlines()

			for i, line in enumerate(content):
				if line.startswith('client_id='):
					content[i] = 'client_id={0}\n'.format(client_id)

				if line.startswith('private_key='):
					content[i] = 'private_key={0}\n'.format(secret_key)

				if line.startswith('report_critical_issues='):
					content[i] = 'report_critical_issues={0}\n'.format(report_issues)
		else:
			content.append('#Genesis client id\n')
			content.append('client_id={0}\n\n'.format(client_id))
			content.append('#Private key\n')
			content.append('private_key={0}\n\n'.format(secret_key))
			content.append('#Report critical issues (default to 1) - set to 0 to turn off this functionality\n')
			content.append('report_critical_issues={0}\n\n'.format(report_issues))
			content.append('#API\'s url\n')
			content.append('api_url=https://vgapistaging.azurewebsites.net/api/CLI/Client/\n\n')
			content.append('#Idle time in seconds\nsleep_time=15\n\n#Maximum parallel uploads\nmax_processes=2\n')

		with open(config_file, 'w+') as file:
			file.writelines(content)


if __name__ == "__main__":
	execution_folder = os.path.dirname(os.path.abspath(__file__))

	args = get_input_parameters()

	#Check if only one option pass 
	v_total_opt = 0
	for k, v in args.items():
		if k in ['start', 'stop', 'restart', 'debug', 'verify', 'configure', 'help'] and v == True:
			v_total_opt += 1

	if v_total_opt != 1:
		logger.error('Can only take one parameter at the same time for actions start, stop, restart or debug')
		sys.exit(1)

	log = False
	if args['log']:
		log = True

	if args['config']:
		config = args['config']
	else:
		config = '{0}/.genesis-config'.format(execution_folder)

	if args['verify']:
		missing = test_install(execution_folder) 
		if len(missing) == 0:
			configuration_file = verify_configuration_file(execution_folder)
			if configuration_file == None:
				sys.stdout.write('WARN: Configuration file not saved yet. Please create {0}/.genesis-config \n'.format(execution_folder))

			sys.stderr.write('Required libraries successfully installed\n')
			sys.exit(0)
		else:
			sys.stderr.write('Error, missing libraries: {0}\n'.format(', '.join(missing)))	
			sys.exit(1)
	elif args['configure']:
		create_config(config)
		sys.stderr.write('Successfully configured!\n')
		sys.exit(0)


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

	logger.setLevel(log_level)

	if args['debug']:
		v_action='debug'
	else:
		v_action='daemon'

	pidfile = '{0}/.pid'.format(execution_folder)
	daemon = GenesisClientCoordinator(config, log=log, log_level=log_level, pidfile=pidfile)

	if args['debug']:
		daemon.run()
	elif args['stop']:
		daemon.stop()
	elif args['start']:
		daemon.start()
	elif args['restart']:
		daemon.stop()
		daemon.start()
	else:
		logger.error('Unknown mode, See details on how to use')
		proc = subprocess.Popen('python {0}/{1} --help'.format(execution_folder, __file__), shell=True)
		proc.communicate()
		sys.exit(1)
