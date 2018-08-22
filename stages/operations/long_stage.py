class {{job}}_{{id}}(luigi.Task):    
    def requires(self):
        return {{job}}_{{parent}}()
    
    def run(self):
        proc = subprocess.run('sleep 30 && cp kkkkk', shell=True)
        proc.check_returncode()
        with open(self.output().path, 'w') as out:
            out.write(str(proc.returncode))

    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/{{id}}.mrk')
