class appConfig(luigi.Config):
    scheduler_wait_time = luigi.IntParameter(default=600)
    scheduler_end_hour = luigi.IntParameter(default=23)
    scheduler_end_minuite = luigi.IntParameter(default=59)


class {{job}}_{{id}}(luigi.Task):
    def requires(self):
        return {{job}}_{{parent}}()

    def run(self):
        wait_time = appConfig().scheduler_wait_time
        end_time = datetime.now().replace(hour=appConfig().scheduler_end_hour, minute=appConfig().scheduler_end_minuite)
        logger.info('{{job}} Configured wait_time:{}, end_time:{}'.format(wait_time, end_time))


        def dbTimeToPyTime(t):
            return datetime.strptime(t[:t.find('.')], '%H:%M:%S').time()

        def getValueFromDfRow(df, row, col):
            i, = np.where(df.columns.values == col)
            return row[i[0]]

        def scanJobTable():
            db = database.DBHelper()
            df = db.getDateByQuery('SELECT * FROM TBLRP_JOBS')
            now = datetime.now()

            monthend = [31,28,31,30,31,30,31,31,30,31,30,31]
            for row in df.values:
                loaded_ok = getValueFromDfRow(df, row, 'UNLOADED') != 'Y'

                load_time = getValueFromDfRow(df, row, 'LOAD_TIME')
                unload_time = getValueFromDfRow(df, row, 'UNLOAD_TIME')
                time_ok = load_time <= now.time() and now.time() <= unload_time

                load_days = getValueFromDfRow(df, row, 'LOAD_DAY').split(',')
                day_ok = str(now.weekday() + 1) in load_days

                this_month_last_day = monthend[now.month - 1]
                if (calendar.isleap(now.year)) :
                    this_month_last_day += 1
                if ('ME' in load_days and now.day == this_month_last_day):
                    day_ok = True

                not_held_ok = getValueFromDfRow(df, row, 'HOLD') != 'Y'

                nrt = getValueFromDfRow(df, row, 'NEXT_RUN_TIME')
                next_run_ok = nrt is None #or math.isnan(nrt)
                if not next_run_ok:
                    next_run_time = nrt
                    next_run_ok = next_run_time <= now.time()

                logger.info('{{job}} load:{}, time:{}, day:{}, next_rune_time:{}, next:{}, held:{}'.format(loaded_ok, time_ok, day_ok, str(nrt), next_run_ok, not_held_ok))
                if loaded_ok and time_ok and day_ok and next_run_ok and not_held_ok:
                    logger.info('{{job}} RUNNING job {}'.format(getValueFromDfRow(df, row, 'JOBNAME')))
                    runJob(db, getValueFromDfRow(df, row, 'JOBID'), getValueFromDfRow(df, row, 'JOBNAME'), getValueFromDfRow(df, row, 'FREQ'))


        def runJob(db, jobid, jobname, freq):
            devnull = subprocess.DEVNULL
            logger.info('{{job}} RUNNING SUBPROCESS ./{}.sh'.format(jobname))
            proc = Popen(['./{}.sh'.format(jobname)], stdout=devnull, stderr=devnull)

            if freq is None or math.isnan(freq):
                # no frequency, stop running in future. Set UNLOADED to Y
                sql = "UPDATE {} SET UNLOADED = 'Y' WHERE JOBID = {}".format('TBLRP_JOBS', jobid)
                db.execStmt(sql)
                return

            ### No need compute, use DB to add time.... // nextrun = datetime.now() + timedelta(seconds=freq)
            # set next run in database
            sql = "UPDATE {} SET NEXT_RUN_TIME = DATEADD(second, {}, getdate()) WHERE JOBID = {}".format('TBLRP_JOBS', freq, jobid)
            db.execStmt(sql)


        while (datetime.now() < end_time):
            scanJobTable()
            time.sleep(wait_time)

        with open(self.output().path, 'w') as out:
            out.write(str('done for the day'))

    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/{{id}}.mrk')
