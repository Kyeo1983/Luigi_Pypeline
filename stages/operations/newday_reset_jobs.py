# Config classes should be camel cased
class {{job}}_{{id}}(luigi.Task):
    def required(self):
        return {{job}}_{{parent}}()

    def run(self):
        logger.info('{{job}} Resetting jobs')

        db = database.DBHelper()
        df = db.getDataByQuery('SELECT * FROM TBLRP_JOBS')
        sql = "UPDATE {} SET NEXT_RUN_TIME = NULL, UNLOADED = 'N' WHERE ISADHOC = 'N' AND JOBNAME <> '{}'".format('TBLRP_JOBS', '{{job}}')
        cnt = db.execStmt(sql)

        # This job only runs at fixed time daily
        sql = "UPDATE {} SET NEXT_RUN_TIME = '{}' WHERE JOBNAME = '{}'".format('TBLRP_JOBS', '00:01:00', '{{job}}')
        db.execStmt(sql)

        with open(self.output().path, 'w') as out:
            out.write(str('Updated #{} of rows.'.format(cnt)))


    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/{{id}}.mrk')
