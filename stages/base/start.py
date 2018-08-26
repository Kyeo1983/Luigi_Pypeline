"""
job_start module my documentation
"""
class job_start:
    """
    job_start class my documentation
    """
    def run(self):
        """
        Initializes settings for a new job flow.
        Sets global sysStatus to 'running'.
        Sets global sysFolder path.        
        """
        

class {{job}}_start(luigi.Task):
    def run(self):

        ctx['sysStatus'] = 'running'
        
        foldername = str(ctx['sysFolder']) + '/run'
        if not os.path.exists(foldername):
            os.makedirs(os.path.join(foldername))
           
        with open(self.output().path, 'w') as out:
            out.write('started successfully')

    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/started.mrk')
