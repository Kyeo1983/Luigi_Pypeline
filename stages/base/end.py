class email(luigi.Config):
    sender = luigi.Parameter(default="luigi-noreply@pypeline.com")
    sendername = luigi.Parameter(default="Luigi")
    receiver = luigi.Parameter()


class stage_end(luigi.Task):
    def requires(self):
        return[{{parent}}]
    
    def run(self):
        foldername = str(ctx['sysFolder'])
        if os.path.exists(foldername):
            shutil.move(os.path.join(foldername + '/run'), os.path.join(foldername + '/run_' + datetime.now().strftime('%Y%m%d%H%M%S')))
        
        emailconf = email()
        subprocess.run('echo "Success" | mail -s "Success" {} -aFrom:{}\<{}\>'.format(emailconf.receiver, emailconf.sendername, emailconf.sender), shell=True)
