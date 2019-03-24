# Config classes should be camel cased
class email(luigi.Config):
    sender = luigi.Parameter(default="luigi-noreply@pypeline.com")
    sendername = luigi.Parameter(default="Mario")

class {{job}}_{{id}}(luigi.Task):
    def required(self):
        return {{job}}_{{parent}}()

    def run(self):
        """ Sends an email with message
        """
        emailconf = email()
        subprocess.call('echo "{}" | mail -s "{}" -r "{}" {}'.format('{{param_in_body}}', '{{param_in_title}}', emailconf.sender, '{{param_in_recipient}}'), shell=True)

        with open(self.output().path, 'w') as out:
            out.write('sent')


    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/{{id}}.mrk')
