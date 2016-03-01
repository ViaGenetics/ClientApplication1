
STATUS  = {
		0: 'active',
		1: 'inactive'
	}

FILE_TYPE = {
		0: 'FASTQ',
		1: 'FASTQ_READ1',
		2: 'FASTQ_READ2',
		3: 'BAM',
		4: 'VCF',
		5: 'GVCF',
	}

TASK_STATUS = {
		0: 'pending',
		1: 'waiting',
		2: 'executing',
		3: 'done',
		4: 'failed',
		5: 'cancelled',
	}


TASK_TYPE = {
		0: 'heartbeat',
		1: 'new_configuration',
		2: 'stop',
		3: 'start',
		4: 'cancel',
		5: 'upload',
		6: 'download',
	}


SAMPLE_DATAFILE_STATUS = {
		0: 'incomplete',
		1: 'pending_upload',
		2: 'transferring',
		3: 'uploaded', 
		4: 'running_bioinformatics', 
		5: 'bioinformatics_done', 
		6: 'combining', 
		7: 'post_bioinformatics',
		8: 'query_ready',
		9: 'inactive', 
		10: 'cancelling', 
		11: 'cancelled',
		12: 'pending_download',
		13: 'downloaded',
		14: 'failed',
		15: 'running_gedi',
		16: 'pending_gedi',
		17: 'gedi_completed',
		18: 'waiting',
		19: 'deleted',
		20: 'failed_unreachable',
	}


class GenesisCommon(object):
    """description of class"""


