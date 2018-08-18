from jinja2 import Environment
from jinja2 import FileSystemLoader
from datetime import datetime
import json
import os
import re
from pathlib import Path


import sys
sys.path.append('../utilities')


BASE_PATH = Path('.')
JOB_MARKER_PATH = BASE_PATH / 'jobmarkers'
JOB_PATH = BASE_PATH / 'jobs'
STAGE_PATH = BASE_PATH / 'stages'
BASE_STAGE_PATH = STAGE_PATH / 'base'
from utilities.configs.importsconf import conf as importsconfig
from utilities.configs.stagesconf import conf as stagesconfig


input_file = json.load(open('./temp/inputs.json', 'r'))
input_name = input_file['name']
input_steps = input_file['sequence']


datefile = datetime.now().strftime('%Y%m%d_%H-%M-%S')
outputfile = JOB_PATH / '{}.py'.format(input_name)
outputbatfile = JOB_PATH / '{}.bat'.format(input_name)


def render(tpl_path, context):
    path, filename = os.path.split(tpl_path)
    return Environment(loader=FileSystemLoader(path or './')).get_template(filename).render(context)

output = open(outputfile, 'w')
outputbat = open(outputbatfile, 'w')


to_import = []
rendered = []
list_job_id = []
list_parent_id = []


rendered.append(open(str(BASE_STAGE_PATH / 'start.py'), "r").read())


for step in input_steps:
    if 'parent' not in step:
        step['parent'] = 'start'
        
    stageconf = stagesconfig[step['stage']]
    ren = render(stageconf['path'], step)
    rendered.append(ren + '\n')
    
    # getting needed imports into list
    to_import = to_import + [i for i in stageconf['dependencies'] if i not in to_import]
    
    # adding the ending
    list_job_id.append(step['id'])
    list_parent_id.append(step['parent'])
    
    
rendered.append(open(str(BASE_STAGE_PATH / 'imports.py'), "r").read())


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
    
sysFolder = '{}/{}'.format(JOB_MARKER_PATH, input_name)
output.write("ctx = {{'sysFolder' : '{}'}}\n".format(sysFolder))


for item in rendered:
    output.write("{}\n".format(item))
    
    
# prep bat file
outputbat.write('python -m luigi --module {} stage_end\n'.format(input_name))
outputbat.close()
output.close()