import sys, os, subprocess, re, time, logging
import requests, json
import signal


from Genesis.GenesisUtils import * 
from Genesis.GenesisHttp import GenesisHttp as GenesisHttp
from Genesis.GenesisCommon import *

logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)
logger.propagate=True

ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(levelname)s - %(asctime)s - %(name)s: %(message)s   '))

logger.addHandler(ch)


class CExternalDataFile(object):

	def __init__(self, configuration, Id, SampleId, FileType, FilePath, FileOrder, Status='incomplete', dnanexus_link= None, log_level=logging.DEBUG ):
		self.log_level = log_level

		logger.setLevel(log_level)

		self.Id= Id
		self.SampleId = SampleId

		try:
			self.FileType = FILE_TYPE[FileType]
		except:
			self.FileType = FileType

		try:
			self.Status = SAMPLE_DATAFILE_STATUS[Status]
		except:
			self.Status = Status		
			
		self.FilePath = FilePath
		self.FileOrder = FileOrder


		self.dnanexus_link = dnanexus_link

		self.client_configuration = configuration


	@property
	def __dict__(self):

		return {
			'Id': self.Id,
			'SampleId': self.SampleId,
			'FileType': self.FileType,
			'Status': self.Status,
			'FilePath': self.FilePath,
			'FileOrder': self.FileOrder,		
			'dnanexus_link':  self.dnanexus_link			
		} 


	def save(self):


		obj_hash = self.__dict__

		try:
			obj_hash['FileType'] = FILE_TYPE[obj_hash['FileType']]
		except:
			obj_hash['FileType'] = obj_hash['FileType']

		try:
			obj_hash['Status'] = SAMPLE_DATAFILE_STATUS[obj_hash['Status']]
		except:
			obj_hash['Status'] = obj_hash['Status']		

		v_partial_url = 'BAM'
		if self.FileType.startswith('FASTQ_') or self.FileType == 'FASTQ':
			v_partial_url='FASTQ'

		v_url = '{0}/{1}/{2}/{3}/Save'.format(self.client_configuration.get('api_url'), self.client_configuration.get('client_id'),  v_partial_url, self.Id)
		http = GenesisHttp()
		resp = http.request(v_url, method='POST', data={'secret_key': self.client_configuration.get('private_key'), 'data': obj_hash}, timeout=30)

		if resp.status_code != 200 :
			logger.error('Status code: %s, Reason: %s', resp.status_code, resp.reason )
			return False
		else:
			return True


class CExternalDataFileVCF(object):
	
	
	def __init__(self, configuration, Id, PostBioinformaticsRunVCFId, FileType, FilePath, FileOrder, Status='incomplete', dnanexus_link= None, DownloadLink=None, log_level=logging.DEBUG ):

		self.log_level = log_level

		logger.setLevel(log_level)

		self.Id = Id
		self.PostBioinformaticsRunVCFId = PostBioinformaticsRunVCFId
		try:
			self.FileType = FILE_TYPE[FileType]
		except:
			self.FileType = FileType

		try:
			self.Status = SAMPLE_DATAFILE_STATUS[Status]
		except:
			self.Status = Status			
			
		self.FilePath = FilePath
		self.FileOrder = FileOrder
	

		self.dnanexus_link = dnanexus_link
		self.DownloadLink = DownloadLink

		self.client_configuration = configuration


	@property
	def __dict__(self):
		
		return {
			'Id': self.Id,
			'PostBioinformaticsRunVCFId': self.PostBioinformaticsRunVCFId,
			'FileType': self.FileType,
			'Status': self.Status,
			'FilePath': self.FilePath,
			'FileOrder': self.FileOrder,		
			'dnanexus_link':  self.dnanexus_link,
			'DownloadLink': self.DownloadLink			
		} 
		

	def save(self):

		obj_hash = self.__dict__

		try:
			obj_hash['FileType'] = FILE_TYPE[obj_hash['FileType']]
		except:
			obj_hash['FileType'] = obj_hash['FileType']

		try:
			obj_hash['Status'] = SAMPLE_DATAFILE_STATUS[obj_hash['Status']]
		except:
			obj_hash['Status'] = obj_hash['Status']		

		v_url = '{0}/{1}/VCF/{2}/Save'.format(self.client_configuration.get('api_url'), self.client_configuration.get('client_id'), self.Id)
		http = GenesisHttp()
		resp = http.request(v_url, method='POST', data={'secret_key': self.client_configuration.get('private_key'), 'data': obj_hash}, timeout=30)

		if resp.status_code != 200 :
			logger.error('Status code: %s, Reason: %s', resp.status_code, resp.reason )
			return False
		else:
			return True


class GenesisTask(object):


	def __init__(self,configuration, task_id, log_level=logging.DEBUG):
		self.log_level = log_level

		logger.setLevel(log_level)

		self.client_configuration=configuration

		self.Id = task_id
		self.GenesisClientAppId = self.client_configuration.get('client_id', None)
		self.Status=None
		self.TaskType='upload'		
		self.AbortTask=False
		self.PID=os.getpid()
		self.ExternalDataFileId = None
		self.ExternalDataFileVCFId = None
		self.ExternalDataFile = None
		self.ExternalDataFileVCF = None
		self.StartTime = None
		self.EndTime = None

		#Exits if task cannot be retrieved
		if not self.map_to_model(self.retrieve()) : 
			logger.error('Failed to retrieve task %s', self.Id)
			sys.exit(1)


	def map_to_model(self, task):
		

		if task != None :

			self.Id = task.get('Id')
			self.PID = task.get('PID')
			self.AbortTask= task.get('AbortTask', False)
			self.StartTime = task.get('StartTime', None)
			self.EndTime = task.get('EndTime', None)
			self.TaskType = TASK_TYPE.get(task.get('TaskType'), task.get('TaskType', 'upload'))

			try:
				self.Status = TASK_STATUS[task.get('Status')]
			except:
				self.Status = task.get('Status')

			try:
				self.TaskType = TASK_TYPE[task.get('TaskType')]
			except:
				self.TaskType = task.get('TaskType')


			if task.get('ExternalDataFile', None) != None:
				self.ExternalDataFileId = task.get('ExternalDataFileId')
				self.ExternalDataFile = CExternalDataFile(self.client_configuration, task.get('ExternalDataFile').get('Id', None), task.get('ExternalDataFile').get('SampleId', None), task.get('ExternalDataFile').get('FileType', None),  task.get('ExternalDataFile').get('FilePath', None), task.get('ExternalDataFile').get('FileOrder', None), task.get('ExternalDataFile').get('Status', None), task.get('ExternalDataFile').get('dnanexus_link', None) )
			else:
				self.ExternalDataFileId = None
				self.ExternalDataFile = None

			if task.get('ExternalDataFileVCF', None) != None:
				self.ExternalDataFileVCFId = task.get('ExternalDataFileVCFId')
				self.ExternalDataFileVCF = CExternalDataFileVCF(self.client_configuration, task.get('ExternalDataFileVCF').get('Id', None), task.get('ExternalDataFileVCF').get('PostBioinformaticsRunVCFId', None), task.get('ExternalDataFileVCF').get('FileType', None),  task.get('ExternalDataFileVCF').get('FilePath', None), task.get('ExternalDataFileVCF').get('FileOrder', None), task.get('ExternalDataFileVCF').get('Status', None), task.get('ExternalDataFileVCF').get('dnanexus_link', None), task.get('ExternalDataFileVCF').get('DownloadLink', None))
			else:
				self.ExternalDataFileVCFId = None
				self.ExternalDataFileVCF = None

	
		else:
			logger.error('Unable to map attributes for Task %s', self.Id)
			return False
				
		return True


	def retrieve(self):
		v_return = None

		v_url = '{0}/{1}/Task/{2}/Load'.format(self.client_configuration.get('api_url'), self.client_configuration.get('client_id'), self.Id)
		http = GenesisHttp()
		resp = http.request(v_url, method='POST', data={'secret_key': self.client_configuration.get('private_key'), 'data': None}, timeout=30)
		
		if resp.status_code == 200:
			v_return = resp.json()
			try:
				v_return['Status'] = TASK_STATUS[v_return['Status']]
			except:
				pass

			try:
				v_return['TaskType'] = TASK_TYPE[v_return['TaskType']]
			except:
				pass
		else:
			logger.error('Unable to retrieve task %s, status code: %s, reason: %s', self.Id, resp.status_code, resp.reason ) 
			return None

		return v_return


	def was_task_cancelled(self):
		v_task = self.retrieve()
		return v_task.get('AbortTask', False)
		

	@property
	def __dict__(self):
		v_ret = {
				'Id': self.Id,
				'Status': self.Status,
				'TaskType': self.TaskType,
				'GenesisClientAppId': self.GenesisClientAppId,
				'AbortTask': self.AbortTask,
				'PID': self.PID,
				'ExternalDataFileId': self.ExternalDataFileId,
				'ExternalDataFileVCFId': self.ExternalDataFileVCFId,
				'StartTime':  self.StartTime,
				'EndTime': self.EndTime		
			}
		
		if self.ExternalDataFile != None:
			v_ret.setdefault('ExternalDataFile', self.ExternalDataFile.__dict__)
			
		if self.ExternalDataFileVCF != None:
			v_ret.setdefault('ExternalDataFileVCF', self.ExternalDataFileVCF.__dict__)

		return v_ret


	def save(self):

		obj_hash = self.__dict__
		try:
			obj_hash['TaskType'] = FILE_TYPE[obj_hash['TaskType']]
		except:
			obj_hash['TaskType'] = obj_hash['TaskType']

		try:
			obj_hash['Status'] = TASK_STATUS[obj_hash['Status']]
		except:
			obj_hash['Status'] = obj_hash['Status']		


		v_url = '{0}/{1}/Task/{2}/Save'.format(self.client_configuration.get('api_url'), self.client_configuration.get('client_id'), self.Id)
		http = GenesisHttp()
		resp = http.request(v_url, method='POST', data={'secret_key': self.client_configuration.get('private_key'), 'data': obj_hash}, timeout=30)
		
		if resp.status_code == 200:
			return resp.json()
		else:
			logger.error('Unable to save task %s, status code: %s, reason: %s', self.Id, resp.status_code, resp.reason ) 
			return None


class GenesisClientTaskTracker(object):


	
	
	def __init__(self, task_id, configuration_file, log_level=logging.DEBUG):
		self.log_level = log_level
		logger.setLevel(log_level)

		MacOSX_folder = ''
		if sys.platform == "darwin":
			MacOSX_folder = "MacOSX/"		

		self.upload_agent='../bin/{0}ua'.format(MacOSX_folder)
		self.compress_files = False
	
		self.execution_folder = os.path.dirname(os.path.abspath(__file__))
		os.chdir(self.execution_folder)

		self.upload_agent=self.upload_agent

		self.task_id = task_id
		self.client_configuration = load_configuration(configuration_file)

		if self.client_configuration == None:
			logger.error('Unable to load configuration file %s', configuration_file)

		self.sleep_time = int(self.client_configuration.get('sleep_time', 15))

		#Exits if there is any missing parameter
		if self.client_configuration.get('api_url', None) == None or self.client_configuration.get('client_id', None) == None or self.client_configuration.get('private_key', None) == None:
			logger.error('Missing parameter. Parameters api_url, client_id and private_key in your configurtion file must have valid values')
			sys.exit(1)
		
		self.task = GenesisTask(self.client_configuration, self.task_id)
		self.client_parameters = self.load_client_parameters()

	
	"""
	Generates Upload Agent commnad to transfer the files over to DNAnexus
	Notes: Will retry up to 10 times if there is a network issue. In case the transfer fails unexpectedly it will attempt to resume uploading
	"""	
	def generate_upload_command(self, file_path, token, project_id, upload_folder='' ):
		v_compress = ''
		if not self.compress_files:
			v_compress = '--do-not-compress'

		return '{0}/{1} -a {2} -p {3} -r 10 {6} -f {4} {5}'.format(self.execution_folder, self.upload_agent, token, project_id, upload_folder, file_path, v_compress)



	def load_client_parameters(self):

		v_url = '{0}/{1}/Parameters'.format(self.client_configuration.get('api_url'), self.client_configuration.get('client_id'))
		http = GenesisHttp()
		resp = http.request(v_url, method='POST', data={'secret_key': self.client_configuration.get('private_key'), 'data': None}, timeout=30)

		if resp.status_code == 200:
			try:
				resp['Status'] = STATUS[resp['Status']]
			except:
				pass
		else:
			logger.error('Unable to load remote parameters, Status code: %s, Reason: %s',  resp.status_code, resp.reason ) 
			return None

		return resp.json()

			
	"""
	Kills process and set task.Status to cancelled
	"""
	def cancel_task(self, proc):

		if not self.task.map_to_model(self.task.retrieve()):
			logger.error('Failed to retrieve task %s', self.task.Id)
			return False

		if self.task.Status == 'executing' or self.task.Status == 'waiting':

			self.task.Status = 'cancelled'
			self.task.EndTime = get_datetime()

			if self.task.save() != None:

				if self.task.ExternalDataFileId != None:
					self.task.ExternalDataFile.Status='cancelled'
					self.task.ExternalDataFile.save()

				if self.task.ExternalDataFileVCFId != None:
					self.task.ExternalDataFileVCF.Status='cancelled'
					self.task.ExternalDataFileVCF.save()

				os.killpg(os.getpgid(proc.pid), signal.SIGTERM)


	"""
	Retrieves DNAnexus file id 
	"""
	def get_dnanexus_file_id(self, v_proc):
		for line in iter(v_proc.stderr.readline, ''):
			if line.strip().startswith('Uploading file '):
				try:
					v_link  = re.search(' to file object (.+?)$', line.strip()).group(1)
				except:
					v_link = None

				return v_link

			if line.strip().startswith('Signature of file ') and line.strip().endswith(' Will resume uploading to it.'):
				try:
					v_link = re.search(' \((.+?)\),', line.strip()).group(1)
				except:
					v_link = None

				return v_link


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


	"""
	Prepares and execute Upload Agent command to transfer the file over to DNAnexus
	"""
	def run(self):
		
		logger.debug('Upload task started %s', self.task.Id)

		#Validations
		if self.client_parameters.get('remote_upload_folder', None) == None or self.client_parameters.get('dnanexus_token', None) == None or self.client_parameters.get('dnanexus_project_id', None) == None :
			logger.error('Missing parameters, contact support')			
			sys.exit(1)

		if self.task.Status != 'waiting':
			logger.error('Unable to process Task %s - wrong Status', self.task.Id)
			sys.exit(1)
		else:
			
			try:

				if self.task.ExternalDataFile != None:
					v_filename = self.task.ExternalDataFile.FilePath

				if self.task.ExternalDataFileVCF != None:
					v_filename = self.task.ExternalDataFileVCF.FilePath

				#Make sure file is accessible
				if not file_stat(v_filename):
					
					if self.task.ExternalDataFile != None:
						self.task.ExternalDataFile.Status='failed_unreachable'
						self.task.ExternalDataFile.save()

					if self.task.ExternalDataFileVCF != None:
						self.task.ExternalDataFileVCF.Status= 'failed_unreachable'
						self.task.ExternalDataFileVCF.save()
										
					self.task.Status='failed'
					self.task.save()
					logger.error('Unable to access file %s for task %s', v_filename, self.task.Id)
					sys.exit(1)


				v_upl_cmd = self.generate_upload_command(v_filename, self.client_parameters.get('dnanexus_token'), self.client_parameters.get('dnanexus_project_id'),  self.client_parameters.get('remote_upload_folder') )
				self.task.Status='executing'
				self.task.StartTime = get_datetime()
				
				if self.task.save() != None:
					
					proc = subprocess.Popen(v_upl_cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE, preexec_fn=os.setsid())

					self.task.PID = proc.pid
					self.task.save()

					if self.task.ExternalDataFileId != None:
						self.task.ExternalDataFile.dnanexus_link = self.get_dnanexus_file_id(proc)
						self.task.ExternalDataFile.Status = 'transferring'
						self.task.ExternalDataFile.save()

					if self.task.ExternalDataFileVCFId != None:
						self.task.ExternalDataFileVCF.dnanexus_link = self.get_dnanexus_file_id(proc)
						self.task.ExternalDataFileVCF.Status = 'transferring'
						self.task.ExternalDataFileVCF.save()

					while proc.poll() is None:
						#Verify if task was cancelled						
						if self.task.was_task_cancelled():
							logger.warning('Task %s was cancelled', self.task.Id)
							self.cancel_task(proc)
							return True

						time.sleep(self.sleep_time)


					self.task.EndTime = get_datetime()

					if proc.returncode != 0:
						self.task.Status = 'failed'
						self.task.save()

						logger.error('Task %s failed', self.task.Id)

						v_error_message = 'Unexpected error ocurred. Make sure your token is valid'.format()

						if int(self.client_configuration.get('report_critical_issues', 0)) == 1:
							self.report_issue(v_error_message)
						else:
							logger.error(v_error_message)


						sys.exit(1)
					else:
						self.task.Status = 'done'

						self.task.save()

						logger.info('Task %s successfully completed', self.task.Id)

				
				else:
					logger.error('Could not set task %s status to executing, trying to rollback it to pending', self.task.Id)
					self.task.Status = 'pending'
					self.task.save()

					sys.exit(1)

			except Exception, e:
				logger.error('Task %s failed due to an unexpected error\n %s', self.task.Id, e)
				self.task.Status='failed'
				self.task.EndTime = get_datetime()
				self.task.save()
				sys.exit(1)

