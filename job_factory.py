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
JOB_PATH = BASE_PATH / 'jobs'
JOB_MARKER_PATH = JOB_PATH / 'jobmarkers'
STAGE_PATH = BASE_PATH / 'stages'
BASE_STAGE_PATH = STAGE_PATH / 'base'
from utilities.configs.importsconf import conf as importsconfig
from utilities.configs.stagesconf import conf as stagesconfig


input_file = json.load(open('./temp/inputs.json', 'r'))
input_name = input_file['name']
input_steps = input_file['sequence']


datefile = datetime.now().strftime('%Y%m%d_%H-%M-%S')
outputfile = JOB_PATH / '{}.py'.format(input_name)
outputbatfile = JOB_PATH / '{}.sh'.format(input_name)


def render(tpl_path, context):
    path, filename = os.path.split(tpl_path)
    return Environment(loader=FileSystemLoader(path or './')).get_template(filename).render(context)


if os.name == 'nt':
    # on windows
    output = open(str(outputfile), 'w')
    outputbat = open(str(outputbatfile), 'w')
    output.write("".join(open(str(BASE_STAGE_PATH / 'imports.py'), "r").readlines()) + "\n")
else:
    output = open(outputfile.resolve(), 'w')
    outputbat = open(outputbatfile.resolve(), 'w')
    output.write("".join(open((BASE_STAGE_PATH / 'imports.py').resolve(), "r").readlines()) + "\n")
    

to_import = []
rendered = []
list_job_id = []
list_parent_id = []


if os.name == 'nt':
    # on windows
    rendered.append(open(str(BASE_STAGE_PATH / 'start.py'), "r").read())
else:
    rendered.append(open((BASE_STAGE_PATH / 'start.py').resolve(), "r").read())


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


list_leaf_stagenames = list(map(lambda leaf_node: "stage_{}()".format(str(leaf_node)), [x for x in list_job_id if x not in list_parent_id]))
end_stg = { 'stage': 'end', 'parent': ','.join(list_leaf_stagenames) }

if os.name == 'nt':
    # on windows
    ren = render(str(BASE_STAGE_PATH / 'end.py'), end_stg)
else:
    ren = render(BASE_STAGE_PATH / 'end.py', end_stg)
rendered.append('{}\n'.format(ren))
    
    
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
    

# Writing Folder to Script
sysFolder = JOB_MARKER_PATH / input_name
if os.name == 'nt':
    # on windows
    sysFolder = str(sysFolder)
else:
    sysFolder = sysFolder.resolve()
output.write("ctx = {{'sysFolder' : '{}'}}\n".format(sysFolder))

# Writing Job Name to Script
output.write("ctx['sysJobName'] = '{}'\n".format(input_name))



for item in rendered:
    output.write("{}\n".format(item))
    
    
# prep bat file
outputbat.write('python -m luigi --module {} stage_end\n'.format(input_name))
outputbat.close()
output.close()
