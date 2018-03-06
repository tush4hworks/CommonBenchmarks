import sys
import subprocess
import json
import logging
import datetime

FORMAT = '%(asctime)-s-%(levelname)s-%(message)s'
logging.basicConfig(format=FORMAT,filename='benchmarks.log',filemode='w',level='INFO')
logger=logging.getLogger(__name__)

def getDateTime():
	return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

def runSystemCommand(cmd,outfile):
	logger.info(cmd)
	logger.info(outfile)
	try:
		logger.info('+ Running '+cmd)
		result=subprocess.check_output(cmd,stderr=subprocess.STDOUT,shell=True)
		with open('History/'+'_'.join([outfile,getDateTime()]),'w+') as f:
			f.write(result)
		logger.info('- Finished executing command '+cmd)
	except Exception as e:
		logger.error('- Finished executing command with exception '+cmd)
		if hasattr(e,'output'):
			with open('History/'+'_'.join([outfile,getDateTime()]),'w+') as f:
				f.write(e.output)

def runTeraGen(setname,options):
	logger.info('+Running teragen for '+setname)
	runSystemCommand('time hadoop jar /usr/hdp/2.6.3.0-235/hadoop-mapreduce/hadoop-mapreduce-examples.jar teragen '+' '.join(['='.join([key,options[key]]) for key in options.keys()])+' 10000000000 /teraInput','_'.join([setname,'teragen']))
	logger.info('-Finished teragen for '+setname)

def cleanTeraGenInput():
	logger.info('+Running clean for '+setname)
	runSystemCommand('hdfs dfs -rm -r /teraInput','cleanTeraInput')
	logger.info('-Finished clean for '+setname)

def runTeraSort(setname,options):
	logger.info('+Running terasort for '+setname)
	runSystemCommand('time hadoop jar /usr/hdp/2.6.3.0-235/hadoop-mapreduce/hadoop-mapreduce-examples.jar '+' '.join(['='.join([key,options[key]]) for key in options.keys()])+' /teraInput /teraOutput','_'.join([setname,'terasort']))
	logger.info('-Finished terasort for '+setname)

def runTeraValidate(setname,options):
	logger.info('+Running teravalidate for '+setname)
	runSystemCommand('time hadoop jar /usr/hdp/2.6.3.0-235/hadoop-mapreduce/hadoop-mapreduce-examples.jar teravalidate '+' '.join(['='.join([key,options[key]]) for key in options.keys()])+' /teraInput /teraOutput','_'.join([setname,'teravalidate']))
	logger.info('-Finished teravalidate for '+setname)

def runTestDFSIO():
	logger.info('+Running TestDFSIO')
	runSystemCommand('time hadoop jar /usr/hdp/2.6.3.0-235/hadoop-mapreduce/hadoop-mapreduce-examples.jar TestDFSIO -write -nrFiles 10 -fileSize 1000','writeDFS')
	runSystemCommand('time hadoop jar /usr/hdp/2.6.3.0-235/hadoop-mapreduce/hadoop-mapreduce-examples.jar TestDFSIO -read -nrFiles 10 -fileSize 1000','readDFS')
	runSystemCommand('time hadoop jar /usr/hdp/2.6.3.0-235/hadoop-mapreduce/hadoop-mapreduce-examples.jar TestDFSIO -clean','cleanDFS')
	logger.info('-Finished TestDFSIO')


if __name__ == '__main__':
	runTestDFSIO()
	for setting in json.loads(open('benchmarkinput.json','r+').read())['wrap']['settings']:
		setname=setting['name']
		cleanTeraGenInput()
		runTeraGen(setname,setting['teragen'])
		runTeraSort(setname,setting['terasort'])
		runTeraValidate(setname,setting['teravalidate'])
	cleanTeraGenInput()
		


