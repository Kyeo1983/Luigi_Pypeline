# Config classes should be camel cased
class {{job}}_{{id}}(luigi.Task):
    def required(self):
        return {{job}}_{{parent}}()

    def run(self):
        """ Sends an email with message and an attachment from local server
        """
        tmpdir = ctx['sysTempDir']
        emailconf = email()
        subprocess.call('echo "Success" | mail -s "{}" -a {} -r "{}<{}>" {}'.format('{{param_in_title}}', {{param_in_filepath}}, emailconf.sendername, emailconf.sender, '{{param_in_recipient}}'), shell=True)

        with open(self.output().path, 'w') as out:
            out.write('sent')


    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/{{id}}.mrk')
