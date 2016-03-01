import unittest#, os, time, re
from Genesis.GenesisUtils import *

class Test_GenesisUtilsTest(unittest.TestCase):
	dummy_file = '~/dummy-file'


	def test_check_process_execution_time(self):
		pid=os.getpid()
		time.sleep(3)
		self.assertGreaterEqual(check_process_execution_time(pid), 1)

	def test_get_internal_ip(self):
		self.assertIsNotNone(get_internal_ip)

	def test_multithreading(self):
		self.assertIsInstance(multithreading(['ls -ltrah']), list)


	def test_get_datetime(self):
		self.assertRegexpMatches(get_datetime(), re.compile('^{0}'.format(datetime.datetime.now().year)))

	def test_write_to_file(self):
		self.assertEqual(write_to_file(self.dummy_file, {}, format='json'), True)

	def test_load_file(self):
		self.assertEqual(load_file(self.dummy_file).strip(), '{}')

	def test_file_stat(self):
		self.assertEqual(file_stat(self.dummy_file), True)

	def test_extract_key_value(self):
		self.assertEqual(extract_key_value('abc=123\n'), {'abc':'123'})

	def test_is_process_active(self):
		pid=os.getpid()
		self.assertGreaterEqual(is_process_active(pid), True)


	def test_load_configuration(self):
		self.assertIsNotNone(load_configuration('.genesis-config'))


if __name__ == '__main__':
	unittest.main()
