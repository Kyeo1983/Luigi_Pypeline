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
            shutil.copyTree(foldername, ctx['sysEndFolder'])

        emailconf = email()
        subprocess.call('echo "Success" | mail -s "Job Success: {}" -r "{}<{}>" {}'.format(ctx['sysJobName'], emailconf.sendername, emailconf.sender, emailconf.receiver), shell=True)

        with open(self.output().path, 'w') as out:
            out.write('ended successfully')

    def output(self):
        return luigi.LocalTarget(ctx['sysRunFolder'] + '/ended.mrk')
