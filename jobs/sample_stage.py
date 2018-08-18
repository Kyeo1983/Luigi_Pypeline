import luigi
import json
import os
import sys
import time
import shutil
from datetime import datetime
sys.path.append('../../utilities')
import pandas as pd
ctx = {'sysFolder' : 'jobs\jobmarkers\sample_stage'}
class stage_start(luigi.Task):
    def run(self):
        ctx['sysStatus'] = 'running'
        
        foldername = str(ctx['sysFolder']) + '/run'
        if not os.path.exists(foldername):
            os.makedirs(os.path.join(foldername))
           
        with open(self.output().path, 'w') as out:
            out.write('started successfully')

    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/started.mrk')
class stage_1(luigi.Task):    
    def requires(self):
        return stage_start()
    
    def run(self):
        myvar = "Within Sample run"
        ctx['_df'] = myvar
        print('>>>> {} <<<<'.format(myvar))
        
        with open(self.output().path, 'w') as out:
            out.write('ran')

    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/1.mrk')

class stage_end(luigi.Task):
    def requires(self):
        return[stage_1()]
    
    def run(self):
        foldername = str(ctx['sysFolder'])
        if os.path.exists(foldername):
            shutil.move(os.path.join(foldername + '/run'), os.path.join(foldername + '/run_' + datetime.now().strftime('%Y%m%d%H%M%S')))

