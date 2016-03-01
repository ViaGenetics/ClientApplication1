import sys, subprocess, time, datetime, json, re, os


"""
For a given process ID calculates the amount of elapsed seconds
Returns: int - seconds 
"""
def check_process_execution_time(pid):
	proc = subprocess.Popen('ps axo pid,etime | sed -e "s/  */ /g" | sed -e "s/^ //g" | grep -e "^{0}\ "  | cut -f2 -d" "'.format(pid), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	v_out, v_err = proc.communicate()

	if proc.returncode == 0:
		v_exec_time_array = v_out.strip().split(':')

		if len(v_exec_time_array) == 1:
		    return int(v_exec_time_array[0])
		elif len(v_exec_time_array) == 2:
		    return (int(v_exec_time_array[0])*60) + int(v_exec_time_array[1])
		elif len(v_exec_time_array) == 3:
		    if '-' not in v_exec_time_array[0]:
		        return (int(v_exec_time_array[0])*3600) + (int(v_exec_time_array[1])*60) + int(v_exec_time_array[2])
		    else:
		        v_day_hour = v_exec_time_array[0].split('-')
		        return ( int(v_day_hour[0])*86400 ) + (int(v_day_hour[1])*3600) + (int(v_exec_time_array[1])*60) + int(v_exec_time_array[2])

"""
Gets execution node's internal IP address 
Returns: string - if any error then returns None
"""
def get_internal_ip():
	proc = subprocess.Popen('ifconfig $1 | grep "inet addr" | awk -F: \'{print $2}\' | awk \'{print $1}\' ', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	out_, err_ = proc.communicate()
	if proc.returncode == 0:
		return out_.split('\n')[0].strip()
	else:
		return None



"""
Executes a set of commands in parallel and return error and out messages along with returned code
Returns: hash {'sysCode':0, 'processes': [{'command': 'ls -ltrah', 'sysCode': 0, 'pid': 123, 'out': '..\n.\n', 'error': None}]}
"""
def multithreading(commands, threads=2, mode= 0, delay=0.3, timeout=None, print_standard_messages=False):
	processes_array = []
	processes = set()    
	for  command_ in commands:
		while len(processes) >= threads:
			tmpProcessesPoll = []
			for p in processes:
				if p.poll() is not None:

					if not print_standard_messages:
						vv_out, vv_err = p.communicate()
					else:
						vv_out = '' 
						vv_err = ''
						p.wait()

					for idx_, proc__ in enumerate(processes_array):
						if proc__['pid'] == p.pid:
							processes_array[idx_]["sysCode"]= p.returncode
							processes_array[idx_]["out"] = vv_out.strip()
							processes_array[idx_]["error"] = vv_err.strip()
							break

					tmpProcessesPoll.append(p)
				else:
					if timeout != None:
						if check_process_execution_time(p.pid) > timeout:
							kill_cmd = subprocess.Popen('kill -9 {0}'.format(p.pid), shell=True )
							kill_cmd.wait()

			processes.difference_update(tmpProcessesPoll)
		time.sleep(delay)

		if not print_standard_messages:
			p_aux = subprocess.Popen(command_, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		else:
			p_aux = subprocess.Popen(command_, shell=True)

		#Will keep the same order as submitted commands  
		processes_array.append({"command": command_, "pid": p_aux.pid })

		processes.add(p_aux)    


	while len(processes) > 0:
		tmpProcessesPoll = []
		for p in processes:
			if p.poll() is not None:

				if not print_standard_messages:
					vv_out, vv_err = p.communicate()
				else:
					vv_out = '' 
					vv_err = ''
					p.wait()

				for idx_, proc__ in enumerate(processes_array):
					if proc__['pid'] == p.pid:
						processes_array[idx_]["sysCode"]= p.returncode
						processes_array[idx_]["out"] = vv_out.strip()
						processes_array[idx_]["error"] = vv_err.strip()
						break

				tmpProcessesPoll.append(p)
			else:
				if timeout != None:
					if check_process_execution_time(p.pid) > timeout:
						kill_cmd = subprocess.Popen('kill -9 {0}'.format(p.pid), shell=True )
						kill_cmd.wait()

		processes.difference_update(tmpProcessesPoll)
		time.sleep(delay)

	#Verify all processes returned codes
	v_ret = 0
	for  v in processes_array:
		if v["sysCode"] != 0 and mode == 0:
			v_ret = v["sysCode"]
			return {"sysCode": v_ret, "processes" : processes_array}

	return {"sysCode": v_ret, "processes" : processes_array}


"""
Gets system date time. If no format it is speficied then will use "%Y-%m-%d %H:%M:%S"
Returns: string
"""
def get_datetime(format=None):
	#"%Y-%m-%d %H:%M:%S"
	if format == None:
		return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	else:
		return datetime.datetime.now().strftime(format)

"""
Saves some content into a file. Will handle json and plain/text format
Returns: True if successfully saved, otherwise returns False
"""
def write_to_file(location, content, format='plain/text', mode='w'):
	with open(location, mode) as out_file: 
		if format == 'json':
			try:
				json.dump(content, out_file)
				out_file.close()
				return True
			except Exception, e:
				raise NameError(e)
		else:
			out_file.write(content)
			out_file.close()
			return True

"""
Reads an entire file content. Handles json format and plain/text.
Returns: string or hash - depending on format parameter
"""
def load_file(location, format='plain/text'):
	object = {}
	with open(location, 'r') as object_data:

		if format == 'json' :
			try:
				object = json.load(object_data)
			except:
				object_data.close()
				raise NameError("ERROR: GenesisUtils.load_file() - Malformed JSON file {0}".format(location))
		else:
			try:
				object = ''
				object = str(object_data.read())
			except Exception, e:
				object_data.close()
				raise NameError(e)

	object_data.close()        
	return object


"""
Perfoms a stat command on a given file.
Returns: True on successful, False if there was an issue accessing the file (not enough privileges or file does not exist).
"""
def file_stat(file_location):
	proc = subprocess.Popen(["stat","{0}".format(file_location)], shell=False,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	out_, error_ = proc.communicate()

	if proc.returncode == 0:
		return True
	else:
		return False



def extract_key_value(keyVal):
	key=None
	value=None

	if keyVal.strip() == '' or keyVal.strip().startswith('#') or keyVal.strip()  == '#' :
		pass
	else:
		tmpKV = keyVal.split('=')
		if len(tmpKV) != 2:
			pass
		else:
			key = tmpKV[0].strip()
			value = tmpKV[1].strip()
	
	return key, value


"""
Parse configuration file
Returns: Dict with configuration file content
"""
def load_configuration(file_location):
	configuration={}
	content = load_file(file_location, format='plain/text')
	lines = content.split('\n')

	for line in lines:
		key, value = extract_key_value(line.strip())
		if key != None and value != None:
			configuration.setdefault(key, value)
	
	if configuration == {}:
		configuration = None

	return configuration


def is_process_active(PID):
	try:
		os.kill(PID, 0)
		return True
	except OSError:
		return False
