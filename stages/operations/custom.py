
{{param_imports}}

class {{job}}_{{id}}(luigi.Task):
    def required(self):
        return {{job}}_{{parent}}()

    def run(self):
        """ Runs a custom code.
        """
        logger.info('{{job}} running custom code')

        {{param_code | replace("\n", "\n        ")}}

        logger.info('{{job}} ending custom code')

        with open(self.output().path, 'w') as out:
            out.write(str('done'))

    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/{{id}}.mrk')
