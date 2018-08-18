class stage_start(luigi.Task):
    def run(self):
        ctx['sysStatus'] = 'running'
        
        foldername = str(ctx['sysFolder']) + './run'
        if not os.path.exists(foldername):
            os.makedirs(os.path.join(foldername))
           
        with open(self.output().path, 'w') as out:
            out.write('started successfully')

    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/started.mrk')