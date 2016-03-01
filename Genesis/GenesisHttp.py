import requests, json, sys, logging

logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)
logger.propagate=True

ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(levelname)s - %(asctime)s - %(name)s: %(message)s   '))

logger.addHandler(ch)


class GenesisHttp(object):
	""""This class handles connections to Genesis API's """


	def __init__(self, verify_certificate=True, log_level=logging.ERROR):
		
		self.request_session = requests.Session()
		self.verify_certificate=verify_certificate



	def request(self, url, method='GET', data=None, headers=None, timeout=None):
		
		v_return = None

		if headers == None:
			headers = dict()		
		
		headers.setdefault('Content-Type', 'application/json')
			
		if method.upper()=='GET':
			response = self.request_session.get(url, headers=headers, verify=self.verify_certificate, timeout=timeout)

		elif method.upper() == 'POST':
			response = self.request_session.post(url, data=json.dumps(data), headers=headers, verify=self.verify_certificate, timeout=timeout)

		elif method.upper() == 'PUT':
			response = self.request_session.put(url=url, data=json.dumps(data), headers=headers, verify=self.verify_certificate, timeout=timeout)

		elif method.upper() == 'PATCH':
			response = self.request_session.patch(url=url, data=json.dumps(data), headers=headers, verify=self.verify_certificate, timeout=timeout)

		elif method.upper() == 'DELETE':
			response = self.request_session.delete(url=url, headers=headers, verify=self.verify_certificate, timeout=timeout)

		elif method.upper() == 'OPTIONS':
			response = self.request_session.options(url=url, headers=headers, timeout=timeout)

		else:
			logger.error("Method '%s' not supported", method)
			return None

		if response.status_code >= 400 :
			logger.error("Unable to access API: %s, reason: %s", url, response.reason)
			return response
		else:
			v_return = response

		return v_return
