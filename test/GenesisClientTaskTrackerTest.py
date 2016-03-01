import unittest
import Genesis.GenesisClientTaskTracker as taskT

class Test_GenesisClientTaskTrackerTest(unittest.TestCase):

	
	
	

	def test_ExternalDataFile(self):
		self.inst_fastq = taskT.ExternalDataFile({}, '1', '123-456', 'FASTQ_READ1', '/path/file.fastq', 1)
		self.assertIsInstance(self.inst_fastq, taskT.ExternalDataFile)

	def test_save_fastq(self):
		self.assertIsNone(self.inst_fastq.save())

	def test_ExternalDataFile_VCF(self):
		self.inst_vcf = taskT.ExternalDataFileVCF({}, '1', '123-456', 'VCF', '/path/file.fastq', 1)
		self.assertIsInstance(self.inst_vcf, taskT.ExternalDataFileVCF)

	def test_save_vcf(self):
		self.assertIsNone(self.inst_vcf.save())

	def test_task(self):
		self.inst_task = taskT.GenesisTask({}, '123')
		self.assertIsInstance(self.inst_task, taskT.GenesisTask)

	def test_retrieve(self):
		self.assertEqual(self.inst_task.retrieve(), None)

	def test_was_cancelled(self):
		self.assertEqual(self.inst_task.was_task_cancelled(), False)

	def test_save_task(self):
		self.assertNotEqual(self.inst_task.save(), None)


	def test_tasktracker(self):
		self.inst_tt = taskT.GenesisClientTaskTracker('123', '.genesis-config')
		self.assertIsInstance(self.inst_tt, taskT.GenesisClientTaskTracker)


	def test_upload_cmd(self):
		self.assertNotEqual(self.inst_tt.generate_upload_command('/path/file', 'token1', 'project-123', '/input'), '')

	def test_load_remote_params(self):
		self.assertEqual(self.inst_tt.load_client_parameters(), None)

	def test_(self):
		pass


if __name__ == '__main__':
    unittest.main()
