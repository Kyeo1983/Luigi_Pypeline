# Config classes should be camel cased
class email(luigi.Config):
    sender = luigi.Parameter(default="luigi-noreply@pypeline.com")
    sendername = luigi.Parameter(default="Mario")
    receiver = luigi.Parameter('kyeo_ses@yahoo.com')


class {{job}}_end(luigi.Task):
    def requires(self):
        return[{{parent}}]
    
    def run(self):
        foldername = str(ctx['sysFolder'])
        if os.path.exists(foldername):
            shutil.move(os.path.join(foldername + '/run'), os.path.join(foldername + '/run_' + datetime.now().strftime('%Y%m%d%H%M%S')))
        
        emailconf = email()
        subprocess.call('echo "Success" | mail -s "Job Success: {}" {} -aFrom:{}\<{}\>'.format(ctx['sysJobName'], emailconf.receiver, emailconf.sendername, emailconf.sender), shell=True)

