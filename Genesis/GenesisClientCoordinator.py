import sys, os, subprocess, re, time, logging
import requests

from Genesis.daemon import *
from Genesis.GenesisUtils import * 
from Genesis.GenesisHttp import GenesisHttp as GenesisHttp
from Genesis.GenesisCommon import *
from Genesis.GenesisClientTaskTracker import GenesisTask as GenesisTask

logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)
logger.propagate=True

ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(levelname)s - %(asctime)s - %(name)s: %(message)s   '))

logger.addHandler(ch)


class GenesisClientCoordinator(Daemon):
	"""Coordinates upload requests tasks"""

	def __init__(self, configuration_file, pidfile='.pid', log=False, log_level=logging.DEBUG):
		
		self.log_level = log_level
		
		logger.setLevel(log_level)

		self.execution_folder = os.path.dirname(os.path.abspath(__file__))
		os.chdir(self.execution_folder)

		self.configuration_file = configuration_file

		self.client_configuration = load_configuration(self.configuration_file)

		self.task_tracker_app = '../tracker.py'
		
		#Max walltime 3 days for ongoing upload tasks
		self.maximum_walltime = 3*86400

		if self.client_configuration == None:
			logger.error('Unable to load configuration file %s', self.configuration_file)
			sys.exit(1)

		if log:
			v_err = '{0}/../err.log'.format(self.execution_folder)
			v_out = '{0}/../out.log'.format(self.execution_folder)
		else:
			v_err = '/dev/null'
			v_out = '/dev/null'

		Daemon.__init__(self, pidfile, stderr=v_err, stdout=v_out)

	def stop(self):
		logger.info('Stopping client coordinator')
		Daemon.stop(self)

	def start(self):
		logger.info('Starting client coordinator')
		Daemon.start(self)



	
	#Get list of pending upload for BAM, FASTQ and VCFs
	def get_pending_tasks(self):

		v_url = '{0}/{1}/Tasks/Pending/Upload'.format(self.client_configuration.get('api_url'), self.client_configuration.get('client_id'))
		
		http = GenesisHttp()
		resp = http.request(v_url, method='POST', data={'secret_key': self.client_configuration.get('private_key'), 'data': None}, timeout=30)

		if resp.status_code == 200:
			return resp.json()
		else:
			logger.error('Unable to load pending tasks - status code: %s, reason: %s',  resp.status_code, resp.reason ) 
			return []


	def get_executing_tasks(self):

		v_url = '{0}/{1}/Tasks/Executing'.format(self.client_configuration.get('api_url'), self.client_configuration.get('client_id'))
		http = GenesisHttp()
		resp = http.request(v_url, method='POST', data={'secret_key': self.client_configuration.get('private_key'), 'data': None}, timeout=30)

		if resp.status_code != 200:
			logger.error('Unable to load executing tasks - status code: %s, reason: %s',  resp.status_code, resp.reason ) 
			return []

		return resp.json()


	def launch_task_tracker(self, task):

		v_task_id = task.get('Id', None)
		try:	

			if v_task_id == None:
				logger.error('Cannot execute task with null Id') 
				return False

			task = GenesisTask(self.client_configuration, v_task_id, log_level=self.log_level)

			task.Status = 'waiting'
			if task.save() == None:
				logger.error('There was an error updating task %s', v_task_id)
				return False

			v_cmd = os.path.abspath('{0}/{1}'.format(self.execution_folder, self.task_tracker_app))
			v_cmd = '{0} -c \'{1}\' -t {2} -l {3}'.format(v_cmd, self.configuration_file, v_task_id, logging.getLevelName(logger.level))

			#logger.debug('Cmd: {0}'.format(v_cmd))

			#Launch subprocess - NOWAIT
			subprocess.Popen(v_cmd, shell=True)

			return True

		except Exception, e:
			logger.error('There was an error launching task tracker for task %s, %s', v_task_id, e)
			task.Status = 'pending'
			task.save()

			return False


	def is_task_orphan(self, task):

		v_pid = task.get('PID', None)
		v_task_id = task.get('Id', None)

		if v_pid == None:
			logger.warning('Process PID cannot be null - task %s', task.get('Id', None)) 
			return False

		if is_process_active(v_pid):
			return False
		else:
			return True
			
	def check_walltime(self, task):

		try:
			v_task_id = task.get('Id')
			v_status = task.get('Status')
			v_start_time = task.get('StartTime')

			if v_task_id == None or v_status == None or v_start_time == None:
				logger.warning('Task Id, StartTime and Status cannot be null') 
				return False
		
			v_end_time = task.get('EndTime', None)

			if v_status == 'executing' and v_end_time == None and abs(datetime.datetime.now() - datetime.datetime.strptime(v_start_time, "%Y-%m-%d %H:%M:%S")).seconds >= self.maximum_walltime:
				logger.warning('Task %s has exceeded the max wall time of %s', task.get('Id', None), self.maximum_walltime) 

		except:
			logger.error('Unable to verify walltime for task %s', task.get('Id', None)) 



	def resume_task(self, task):

		v_task_id = task.get('Id', None)
		if v_task_id == None:
			logger.error('Cannot resume task with null Id') 
			return False

		task = GenesisTask(self.client_configuration, v_task_id)
		task.Status = 'pending'

		if task.save() != None:
			return True
		else:
			logger.warning('unable to reset task %s', task.get('Id', None)) 
			return False


	def report_issue(self, message):

		v_url = '{0}/{1}/Report/Issue'.format(self.client_configuration.get('api_url'), self.client_configuration.get('client_id'))
		http = GenesisHttp()
		data = {'Message': message}
		resp = http.request(v_url, method='POST', data={'secret_key': self.client_configuration.get('private_key'), 'data': data}, timeout=30)

		if resp.status_code == 200:
			return True
		else:
			logger.error('Unable to report issue - status code: %s, reason: %s',  resp.status_code, resp.reason ) 
			return False


	def run(self):

		while 1 == 1:
			try:

				#Get initial list of running tasks to verify if they are still active
				v_running_tasks = self.get_executing_tasks()
				for task in v_running_tasks:
					if self.is_task_orphan(task):
						if self.resume_task(task):
							logger.info('Task %s was found orphan and was sucessfully resumed', task.get('Id', None))
							
					else:
						self.check_walltime(task)
				
				#Get running tasks after resuming orphan processes
				v_running_tasks = self.get_executing_tasks()

				#Load pending tasks
				v_pending_tasks = self.get_pending_tasks()
				if len(v_pending_tasks) == 0:
					logger.info('No pending upload tasks found at this time')


				#Launch pending tasks
				for pending_task in v_pending_tasks:



					if TASK_STATUS.get(pending_task.get('Status', None), None) == 'pending':
						if len(v_running_tasks) == int(self.client_configuration.get('max_processes', 4)):
							break 

						logger.info('Processing pending task %s ', pending_task.get('Id', None) )

						if self.launch_task_tracker(pending_task):
							v_running_tasks.append(pending_task)
						else:
							v_error_message = 'Unable to launch task {0}'.format(pending_task.get('Id', None))

							if int(self.client_configuration.get('report_critical_issues', 0)) == 1:

								self.report_issue(v_error_message)
							else:
								logger.error(v_error_message)


			except Exception, e:
				logger.error('%s', e)
				logger.error('Coordinator failed, will sleep for %s seconds before retrying', str(self.client_configuration.get('sleep_time', 10)) )
				

			time.sleep(int(self.client_configuration.get('sleep_time', 10)))


