import os
import os.path
import yaml
from pathlib import Path
import sys
import traceback
import zipfile
import luigi
import luigi.local_target
import pieshell
import traceback
import math
import time
import requests
import datetime
import poltergust_luigi_utils # Add GCS luigi opener
import poltergust_luigi_utils.logging_task
import poltergust_luigi_utils.gcs_opener

DB_URL = os.environ.get("DB_URL")

def strnow():
    return datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S.%f")

def make_environment(envpath, environment, log):
    _ = pieshell.env(exports=dict(pieshell.env._exports))
    if not os.path.exists(envpath):
        envdir = os.path.dirname(envpath)
        if not os.path.exists(envdir):
            os.makedirs(envdir)
        +_.virtualenv(envpath, **environment.get("virtualenv", {}))
    +_.bashsource(envpath + "/bin/activate")
    for dep in environment["dependencies"]:
        for line in _.pip.install(dep):
            log(line)

def zip_dir(envdir, log):
    archive = envdir + ".zip"
    envdir = Path(envdir)
    with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=6) as f:
        for file in envdir.rglob('*'):
            f.write(file, arcname=file.relative_to(envdir))
        log("Zipped: {}".format(f))
    return archive


def download_environment(envpath, path, log):
    if not os.path.exists(envpath):
        envdir = os.path.dirname(envpath)
        if not os.path.exists(envdir):
            os.makedirs(envdir)

    with poltergust_luigi_utils.client.download(path) as z:
        with zipfile.ZipFile(z, mode="r") as arc:
            arc.extractall(envpath)
        log(arc.printdir())


class MakeEnvironment(poltergust_luigi_utils.logging_task.LoggingTask, luigi.Task):
    path = luigi.Parameter()
    hostname = luigi.Parameter()
    retry_on_error = luigi.Parameter(default=False)

    def run(self):
        with self.logging(self.retry_on_error):
            zip_path = str(self.path) + ".zip"
            if poltergust_luigi_utils.gcs_opener.client.exists(zip_path):
                download_environment(self.envdir().path, zip_path, self.log)
            else:
                with luigi.contrib.opener.OpenerTarget(self.path).open("r") as f:
                    environment = yaml.load(f, Loader=yaml.SafeLoader)
                    make_environment(self.envdir().path, environment, self.log)
                    with self.output().open("w") as f:
                        f.write("DONE")        
                archived_env = zip_dir(self.envdir().path, self.log)
                poltergust_luigi_utils.gcs_opener.client.put(archived_env, zip_path)


    def envdir(self):
        return luigi.local_target.LocalTarget(
            os.path.join("/tmp/environments", self.path.replace("://", "/").lstrip("/")))

    def logfile(self):
        return luigi.contrib.opener.OpenerTarget("%s.%s.log.txt" % (self.path, self.hostname))
            
    def output(self):
        return luigi.local_target.LocalTarget(
            os.path.join("/tmp/environments", self.path.replace("://", "/").lstrip("/"), "done"))
        
class RunTask(poltergust_luigi_utils.logging_task.LoggingTask, luigi.Task):
    path = luigi.Parameter()
    hostname = luigi.Parameter()
    retry_on_error = luigi.Parameter(default=False)
    
    @property
    def scheduler(self):
        return self.set_progress_percentage.__self__._scheduler

    @property
    def scheduler_url(self):
        return self.scheduler._url
    
    def run(self):
        with self.logging(self.retry_on_error):
            self.log('RunTask start')

            src = '%s.config.yaml' % (self.path,)
            fs = self.output().fs

            try:
                cfg = luigi.contrib.opener.OpenerTarget('%s.config.yaml' % (self.path,))
                try:
                    with cfg.open("r") as f:
                        task = yaml.load(f, Loader=yaml.SafeLoader)
                except:
                    if not cfg.fs.exists(cfg.path):
                        # The task has already been marked as done by another worker.
                        return
                    raise
                self.log('RunTask config loaded')

                with luigi.contrib.opener.OpenerTarget(task["environment"]).open("r") as f:
                    environment = yaml.load(f, Loader=yaml.SafeLoader)        

                env = MakeEnvironment(path=task["environment"], hostname=self.hostname, retry_on_error=self.retry_on_error)
                if not env.output().exists():
                    yield env
                envpath = env.envdir().path
                self.log('RunTask environment made')

                _ = pieshell.env(envpath, interactive=True)
                +_.bashsource(envpath + "/bin/activate")

                _._exports.update(environment.get("variables", {}))
                _._exports.update(task.get("variables", {}))

                self.log('RunTask loaded environment')

                command = task.get("command", None)

                task_args = dict(task.get("task", {}))
                task_name = task_args.pop("name", None)
                task_args["scheduler-url"] = self.scheduler_url
                task_args["retcode-already-running"] = "10"
                task_args["retcode-missing-data"] = "20"
                task_args["retcode-not-run"] = "25"
                task_args["retcode-task-failed"] = "30"
                task_args["retcode-scheduling-error"] = "35"
                task_args["retcode-unhandled-exception"] = "40"

                if command is None:
                    command = "luigi(task_name, **task_args)"

                scope = pieshell.environ.EnvScope(env=_)
                scope["task_name"] = task_name
                scope["task_args"] = task_args

                self.log('RunTask starting actual task' + strnow())

                # Rerun the task until it is actually being run (or is done!)
                # We need to do this, since luigi might exit because all
                # dependencies of a task are already being run by other
                # workers, and our task can't even start...
                while True:
                    try:
                        for line in eval(command, scope):
                            self.log(line)
                    except pieshell.PipelineFailed as e:
                        self.log(str(e))
                        if e.pipeline.exit_code <= 25:
                            time.sleep(5)
                            continue
                        raise
                    break

                self.log('RunTask end')
                dst = '%s.done.yaml' % (self.path,)

            except Exception as e:
                dst = '%s.error.yaml' % (self.path,)
            
        src = '%s.config.yaml' % (self.path,)
        fs = self.output().fs
        try:
            fs.move(src, dst)
        except:
            # Might already have been moved by another node...
            pass

        if DB_URL is not None:
            r = requests.get(DB_URL, params={"pipeline_url": self.path})
            if r.status_code != 200:
                print("Unable to update status", r.status_code, r.text)
        
    def logfile(self):
        return luigi.contrib.opener.OpenerTarget("%s.%s.log.txt" % (self.path, self.hostname))

    def output(self):
         return luigi.contrib.opener.OpenerTarget(
             '%s.done.yaml' % (self.path,))

        
class RunTasks(luigi.Task):
    path = luigi.Parameter()
    hostname = luigi.Parameter()

    def run(self):
        while True:
            yield [RunTask(path=path.replace(".config.yaml", ""), hostname=self.hostname)
                   for path in self.output().fs.list_wildcard('%s/*.config.yaml' % (self.path,))]
            time.sleep(1)
    
    def output(self):
        # This target should never actually be created...
        return luigi.contrib.opener.OpenerTarget('%s/done' % (self.path,))
    

# luigi --module emerald_algorithms_evaluation.luigi Pipeline --param-evaluation_name=test-luigi-redhog-1
