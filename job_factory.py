import json
import os
import re
import sys
import logging
import logging.config
from pathlib import Path
from datetime import datetime
from jinja2 import Environment
from jinja2 import FileSystemLoader
sys.path.append('../utilities')
from utilities.helper.ospath import OSPath



# Setting up base configurations
BASE_PATH = Path('.')
LOG_CONFIG_PATH = BASE_PATH / 'luigi_central_scheduler' / 'luigi_log.cfg'
STAGE_LOGGER = 'luigi-interface'
JOB_PATH = BASE_PATH / 'jobs'
JOB_MARKER_PATH = JOB_PATH / 'jobmarkers'
STAGE_PATH = BASE_PATH / 'stages'
BASE_STAGE_PATH = STAGE_PATH / 'base'
from utilities.configs.importsconf import conf as importsconfig
from utilities.configs.stagesconf import conf as stagesconfig


# Prepare logger for job factory
logCfgPath = OSPath.path(LOG_CONFIG_PATH)
logging.config.fileConfig(logCfgPath)
logger = logging.getLogger('job.factory')


# Reading input file
filename = './temp/inputs.json'
logger.info('Reading from file {}'.format(filename))
input_file = json.load(open(filename, 'r'))
job_name = input_file['name']
input_steps = input_file['sequence']


# Generate output file names
datefile = datetime.now().strftime('%Y%m%d_%H-%M-%S')
outputfile = JOB_PATH / '{}.py'.format(job_name)
outputbatfile = JOB_PATH / '{}.sh'.format(job_name)
logger.info('Output py file: {}'.format(outputfile))


def render(tpl_path, context):
    path, filename = os.path.split(tpl_path)
    return Environment(loader=FileSystemLoader(path or './')).get_template(filename).render(context)

    
output = open(OSPath.path(outputfile), 'w')
outputbat = open(OSPath.path(outputbatfile), 'w')
output.write("".join(open(OSPath.path(BASE_STAGE_PATH / 'imports.py'), "r").readlines()) + "\n")


to_import = []
rendered = []
list_job_id = []
list_parent_id = []


# Write Job.start stage
rendered.append(render(OSPath.path(BASE_STAGE_PATH / 'start.py'), { "job": job_name }))
logger.info('Written job {} start stage.'.format(job_name))


for step in input_steps:
    if 'parent' not in step:
        step['parent'] = 'start'
    
    step['job'] = job_name
    stageconf = stagesconfig[step['stage']]
    ren = render(stageconf['path'], step)
    rendered.append(ren + '\n')
    logger.info('Written stage ({}) for job : {}.'.format(step['stage'], job_name))

    # getting needed imports into list
    to_import = to_import + [i for i in stageconf['dependencies'] if i not in to_import]
    
    # adding the ending
    list_job_id.append(step['id'])
    list_parent_id.append(step['parent'])

# Adding last (end) stage that is dependency on all other jobs 
list_leaf_stagenames = list(map(lambda leaf_node: "{}_{}()".format(job_name, str(leaf_node)), [x for x in list_job_id if x not in list_parent_id]))
end_stg = { 'job': job_name, 'stage': 'end', 'parent': ','.join(list_leaf_stagenames) }

ren = render(OSPath.path(BASE_STAGE_PATH / 'end.py'), end_stg)
rendered.append('{}\n'.format(ren))
logger.info('Written end stage for job : {}.'.format(job_name))
    
    
# looping through import list and write import lines
for imports in to_import:
    importconf = importsconfig[imports]
    importstr = ''
    if 'importclass' in importconf:
        importstr = 'from {} import {}'.format(importconf['package'], importconf['importclass'])
    else:
        importstr = 'import {}'.format(importconf['package'])
    
    if 'alias' in importconf:
        importstr = '{} as {}'.format(importstr, importconf['alias'])        
    output.write('{}\n'.format(importstr))
logger.info('Written all imports for job')    
    

# Writing top-level job and config specific data to global param in workflow   
# 1. Writing Folder to Script
sysFolder = OSPath.path(JOB_MARKER_PATH / job_name)
output.write("ctx = {{'sysFolder' : '{}'}}\n".format(sysFolder))
# 2. Writing Job Name to Script
output.write("ctx['sysJobName'] = '{}'\n".format(job_name))
# 3. Writing Logger to Script
output.write("ctx['sysLogConfig'] = '{}'\n".format(logCfgPath))
output.write("logging.config.fileConfig(ctx['sysLogConfig'])\n")
output.write("logger = logging.getLogger({})\n".format(STAGE_LOGGER))


for item in rendered:
    output.write("{}\n".format(item))
    
    
# prep bat file
outputbat.write('#!/bin/sh\n')
outputbat.write('python -m luigi --module {} {}_end\n'.format(job_name, job_name))
outputbat.close()
output.close()
logger.info('Job ({}) creation completed.'.format(job_name))