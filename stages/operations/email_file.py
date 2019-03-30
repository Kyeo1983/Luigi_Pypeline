# Config classes should be camel cased
class {{job}}_{{id}}(luigi.Task):
    def required(self):
        return {{job}}_{{parent}}()

    def run(self):
        """ Sends an email with message and an attachment from local server
        """
        tmpdir = ctx['sysTempDir']
        emailconf = email()
        smtpconf = smtp()
        cmd = 'echo "Success" | s-nail -s "{}" -a {} -r "{}" -S smtp="{}:{}" -S smtp-use-starttls -S smtp-auth=login -S smtp-auth-user="{}" -S smtp-auth-password="{}" -S ssl-verify=ignore {}'.format('{{param_in_title}}', {{param_in_filepath}}, emailconf.sender, smtpconf.host, smtpconf.port, smtpconf.username, smtpconf.password, '{{param_in_recipient}}')
        subprocess.call(cmd, shell=True)

        with open(self.output().path, 'w') as out:
            out.write('sent')


    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/{{id}}.mrk')
