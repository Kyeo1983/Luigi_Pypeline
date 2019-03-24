# Config classes should be camel cased
class email(luigi.Config):
    sender = luigi.Parameter(default="luigi-noreply@pypeline.com")
    sendername = luigi.Parameter(default="Mario")
    receiver = luigi.Parameter('kyeo_ses@yahoo.com')
    smtpPW = luigi.Parameter()
    smtpUID = luigi.Parameter()
    smtpHost = luigi.Parameter()
    smtpPort = luigi.Parameter()


class {{job}}_end(luigi.Task):
    def requires(self):
        return[{{parent}}]

    def run(self):
        foldername = str(ctx['sysFolder'])
        if os.path.exists(foldername):
            shutil.copyTree(foldername, ctx['sysEndFolder'])

        emailconf = email()
        subprocess.call('echo "Success" | s-nail -s "Job Success: {}" -r "{}<{}>" -S smtp="{}:{}" -S smtp-use-starttls -S smtp-auth-login -S smtp-auth-user="{}" -S smtp-auth-password="{}" -S ssl-verify=ignore {}'.format(ctx['sysJobName'], emailconf.sendername, emailconf.sender, emailconf.smtpHost, emailconf.smtpPort, emailconf.smtpUID, emailconf.smtpPW, emailconf.receiver), shell=True)


        with open(self.output().path, 'w') as out:
            out.write('ended successfully')

    def output(self):
        ctx['sysEndFolder'] = os.path.join(ctx['sysRunFolder'] + '_' + datetime.now().strftime('%Y%m%d%H%M%S'))
        #Make directories if not exists
        for f in ['sysFolder', 'sysRunFolder',' sysEndFolder']:
            foldername = str(ctx[f])
            if not os.path.exists(foldername):
                os.makedirs(os.path.join(foldername)) 
        
        return luigi.LocalTarget(ctx['sysRunFolder'] + '/ended.mrk')
