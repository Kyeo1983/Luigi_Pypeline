import luigi, json, os, sys, time, shutil, subprocess, logging
import logging.config
from datetime import datetime
####################################
# BASE LIBRARIES REQUIRED BY SCRAPER
####################################
import requests
from pathlib import Path
import pandas as pd
sys.path.append('../utils')
sys.path.append('../configs')
from appconf import conf

project_id = conf["project_id"]
workingdir = conf["working_dir"]
ctx = {'sysFolder' : workingdir + '/jobs/jobmarkers/sample_stage'}
ctx['sysRunFolder'] = workingdir + '/jobs/jobmarkers/sample_stage/run'
ctx['sysSaveFolder'] = workingdir + '/jobs/jobmarkers/sample_stage/run/save'
ctx['sysJobName'] = 'sample_stage'
ctx['sysLogConfig'] = workingdir + '/luigi_central_scheduler/luigi_log.cfg'
logging.config.fileConfig(ctx['sysLogConfig'])
logger = logging.getLogger('luigi-interface')


# Config classes should be camel cased
class email(luigi.Config):
    sender = luigi.Parameter(default="Luigi<luigi-noreply@pypeline.com>")
    receiver = luigi.Parameter()
class smtp(luigi.Config):
    password = luigi.Parameter()
    username = luigi.Parameter()
    host = luigi.Parameter()
    port = luigi.Parameter()


#########################################################################################
############################### LIST GLOBAL VARIABLES HERE ##############################
#########################################################################################


########################################
# START LUIGI PIPELINE
########################################
class sample_stage_start(luigi.Task):
    def run(self):
        ctx['sysStatus'] = 'running'
        ctx['sysTempFolder'] = ctx['sysRunFolder']
        with open(self.output().path, 'w') as out:
            out.write('started successfully')

    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/started.mrk')


class sample_stage_1(luigi.Task):
    def requires(self):
        return sample_stage_start()

    def run(self):
        myvar = "Within Sample run"
        ctx['_df'] = myvar
        print('>>>> {} <<<<'.format(myvar))

        with open(self.output().path, 'w') as out:
            out.write('ran')

    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/1.mrk')

class sample_stage_2(luigi.Task):
    def requires(self):
        return sample_stage_start()

    def run(self):
        # proc = subprocess.run('sleep 30 && cp kkkkk', shell=True)
        proc = subprocess.run('sleep 10', shell=True)
        proc.check_returncode()
        with open(self.output().path, 'w') as out:
            out.write(str(proc.returncode))

    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/2.mrk')



class sample_stage_end(luigi.Task):
    def requires(self):
        return[sample_stage_2()]

    def run(self):
        foldername = str(ctx['sysFolder'])
        if os.path.exists(foldername):
            shutil.copytree(foldername, ctx['sysEndFolder'])

        emailconf = email()
        smtpconf = smtp()
        cmd = 'echo "Success" | s-nail -s "Job Success: {}" -r "{}" -S smtp="{}:{}" -S smtp-use-starttls -S smtp-auth=login -S smtp-auth-user="{}" -S smtp-auth-password="{}" -S ssl-verify=ignore {}'.format(ctx['sysJobName'], emailconf.sender, smtpconf.host, smtpconf.port, smtpconf.username, smtpconf.password, emailconf.receiver)
        subprocess.call(cmd, shell=True)


        with open(self.output().path, 'w') as out:
            out.write('ended successfully')

    def output(self):
        global ctx

        #Make directories if not exists
        ctx['sysEndFolder'] = os.path.join(ctx['sysRunFolder'] + '_' + datetime.now().strftime('%Y%m%d%H%M%S'))
        for f in ['sysFolder', 'sysRunFolder']:
            foldername = str(ctx[f])
            if not os.path.exists(foldername):
                os.makedirs(os.path.join(foldername))

        return luigi.LocalTarget(ctx['sysRunFolder'] + '/ended.mrk')
