class appConfig(luigi.Config):
    wait_time = luigi.IntParameter(default=600)

    
class {{job}}_{{id}}(luigi.Task):
    def requires(self):
        return {{job}}_{{parent}}()
    
    
    def scanJobTable():
        dbcol = { 'JOBID':0, 'JOBNAME':1, 'UNLOADED':2, 'LOAD_DAY':3, 'LOAD_TIME':4, 'UNLOAD_TIME':5, 'FREQ':6, 'NEXT_RUN_TIME':7 }
        
        df = pd.read_csv('../temp/jobtable.csv')
        now = datetime.now()
        for row in df.values:
            loaded_ok = row[dbcol.UNLOADED] != 'Y'

            load_time = datetime.strptime(row[dbcol.LOAD_TIME], '%H:%M').time()
            unload_time = datetime.strptime(row[dbcol.UNLOAD_TIME], '%H:%M').time()
            time_ok = load_time <= now.time() and now.time() <= unload_time

            load_days = row[dbcol.LOAD_DAY].split(',')
            day_ok = str(now.weekday() + 1) in load_days

            next_run_ok = row[dbcol.NEXT_RUN_TIME] is None or math.isnan(row[dbcol.NEXT_RUN_TIME])
            if not next_run_ok:
                next_run_time = datetime.strptime(row[dbcol.NEXT_RUN_TIME], '%H:%M').time()
                next_run_ok = next_run_time.time() <= now.time()

            if loaded_ok and time_ok and day_ok and next_run_ok:
                runJob(row[dbcol.JOBNAME], row[dbcol.FREQ])
        
    
    def runJob(jobname, freq):
        devnull = subprocess.DEVNULL
        proc = Popen(['./{}.sh'.format(jobname)], stdout=devnull, stderr=devnull)

        if freq is None or math.isnan(freq):
            # TODO, no frequency, set UNLOADED to Y        
            a = "UPDATE db SET UNLOADED = 'Y'"
        nextrun = datetime.now() + timedelta(seconds=freq)
        # TODO set next run in database
    
    
    def run(self):
        wait_time = appConfig().waitTime
        end_time = datetime.now().replace(hour=22, minute=0)

        while (datetime.now() < end_time):
            scanJobTable()
            time.sleep(wait_time)
        
        with open(self.output().path, 'w') as out:
            out.write(str('done for the day'))

    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/{{id}}.mrk')