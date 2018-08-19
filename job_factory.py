from jinja2 import Environment
from jinja2 import FileSystemLoader
from datetime import datetime
import json
import os
import re
from pathlib import Path


import sys
sys.path.append('../utilities')
from utilities.helper.ospath import OSPath


BASE_PATH = Path('.')
JOB_PATH = BASE_PATH / 'jobs'
JOB_MARKER_PATH = JOB_PATH / 'jobmarkers'
STAGE_PATH = BASE_PATH / 'stages'
BASE_STAGE_PATH = STAGE_PATH / 'base'
from utilities.configs.importsconf import conf as importsconfig
from utilities.configs.stagesconf import conf as stagesconfig


input_file = json.load(open('./temp/inputs.json', 'r'))
job_name = input_file['name']
input_steps = input_file['sequence']


datefile = datetime.now().strftime('%Y%m%d_%H-%M-%S')
outputfile = JOB_PATH / '{}.py'.format(job_name)
outputbatfile = JOB_PATH / '{}.sh'.format(job_name)


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



rendered.append(render(OSPath.path(BASE_STAGE_PATH / 'start.py'), { "job": job_name }))


for step in input_steps:
    if 'parent' not in step:
        step['parent'] = 'start'
    
    step['job'] = job_name
    stageconf = stagesconfig[step['stage']]
    ren = render(stageconf['path'], step)
    rendered.append(ren + '\n')
    
    # getting needed imports into list
    to_import = to_import + [i for i in stageconf['dependencies'] if i not in to_import]
    
    # adding the ending
    list_job_id.append(step['id'])
    list_parent_id.append(step['parent'])


list_leaf_stagenames = list(map(lambda leaf_node: "{}_{}()".format(job_name, str(leaf_node)), [x for x in list_job_id if x not in list_parent_id]))
end_stg = { 'job': job_name, 'stage': 'end', 'parent': ','.join(list_leaf_stagenames) }

ren = render(OSPath.path(BASE_STAGE_PATH / 'end.py'), end_stg)
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
sysFolder = OSPath.path(JOB_MARKER_PATH / job_name)
output.write("ctx = {{'sysFolder' : '{}'}}\n".format(sysFolder))


# Writing Job Name to Script
output.write("ctx['sysJobName'] = '{}'\n".format(job_name))


for item in rendered:
    output.write("{}\n".format(item))
    
    
# prep bat file
outputbat.write('python -m luigi --module {} {}_end\n'.format(job_name, job_name))
outputbat.close()
output.close()
