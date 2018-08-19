class {{job}}_{{id}}(luigi.Task):    
    def requires(self):
        return {{job}}_{{parent}}()
    
    def run(self):
        r = subprocess.call('sleep 30 || ls /home/eeee', shell=True)
        with open(self.output().path, 'w') as out:
            out.write(str(r))

    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/{{id}}.mrk')
