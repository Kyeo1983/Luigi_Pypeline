# Config classes should be camel cased
class email(luigi.Config):
    sender = luigi.Parameter(default="luigi-noreply@pypeline.com")
    sendername = luigi.Parameter(default="Mario")
    receiver = luigi.Parameter('kyeo_ses@yahoo.com')

class smtp(luigi.Config):
    password = luigi.Parameter()
    username = luigi.Parameter()
    host = luigi.Parameter()
    port = luigi.Parameter()


class {{job}}_end(luigi.Task):
    def requires(self):
        return[{{parent}}]

    def run(self):
        global ctx
        foldername = str(ctx['sysFolder'])
        if os.path.exists(foldername):
            shutil.copyTree(foldername, ctx['sysEndFolder'])

        emailconf = email()
        smtpconf = smtp()
        subprocess.call('echo "Success" | s-nail -s "Job Success: {}" -r "{}<{}>" -S smtp="{}:{}" -S smtp-use-starttls -S smtp-auth-login -S smtp-auth-user="{}" -S smtp-auth-password="{}" -S ssl-verify=ignore {}'.format(ctx['sysJobName'], emailconf.sendername, emailconf.sender, smtpconf.host, smtpconf.port, smtpconf.username, smtpconf.password, emailconf.receiver), shell=True)

        with open(self.output().path, 'w') as out:
            out.write('ended successfully')

    def output(self):
        global ctx

        #Make directories if not exists
        ctx['sysEndFolder'] = os.path.join(ctx['sysRunFolder'] + '_' + datetime.now().strftime('%Y%m%d%H%M%S'))
        for f in ['sysFolder', 'sysRunFolder', 'sysEndFolder']:
            foldername = str(ctx[f])
            if not os.path.exists(foldername):
                os.makedirs(os.path.join(foldername))

        return luigi.LocalTarget(ctx['sysRunFolder'] + '/ended.mrk')
