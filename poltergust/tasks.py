import os
import os.path
import yaml
import traceback
import luigi
import luigi.contrib.opener
import luigi.local_target
import pieshell
import traceback
import time
import requests
import poltergust_luigi_utils # Add GCS luigi opener
import poltergust_luigi_utils.logging_task
import poltergust_luigi_utils.gcs_opener
import fnmatch

from .lib import *

class KillTask(luigi.Task, poltergust_luigi_utils.logging_task.LoggingTask):
    accepts_messages = True
    #priority = 10

    def run(self):
        while True:
            if not self.scheduler_messages.empty():
                msg = self.scheduler_messages.get()
                content = msg.content
                if content.startswith("kill"):
                    args = content.split(":")
                    pid = args[1]
                    assert pid != os.getpid(), msg.respond("Not me!")

                    cfg = args[2]
                    status = remove_config_from_pipeline(cfg)
                    msg.respond(status)

                    gone = kill_proc_tree(int(pid), sig=signal.SIGTERM)
                    print(gone)
                    self.log(gone)
                    msg.respond(f"Killed: {pid}")
                else:
                    msg.respond("Unknown message!")
                time.sleep(1)
                
    def output(self):
        return luigi.local_target.LocalTarget('killed_tasks.pid')



class MakeEnvironment(poltergust_luigi_utils.logging_task.LoggingTask, luigi.Task):
    path = luigi.Parameter()
    hostname = luigi.Parameter()
    retry_on_error = luigi.Parameter(default=False)

    def run(self):
        with self.logging(self.retry_on_error):
            zip_path = str(self.path) + ".zip"
            if DOWNLOAD_ENVIRONMENT:
                if luigi.contrib.opener.OpenerTarget(zip_path).exists():
                    download_environment(self.envdir().path, zip_path, self.log)
            else:
                with luigi.contrib.opener.OpenerTarget(self.path).open("r") as f:
                    environment = yaml.load(f, Loader=yaml.SafeLoader)
                    print(environment)
                    make_environment(self.envdir().path, environment, self.log)
                    with self.output().open("w") as f:
                        f.write("DONE")        
                if UPLOAD_ENVIRONMENT:
                    archive = zip_environment(self.envdir().path, self.log)
                    fs = luigi.contrib.opener.OpenerTarget(self.path).fs
                    fs.put(archive, zip_path)


    def envdir(self):
        return luigi.local_target.LocalTarget(
            os.path.join(ENVIRONMENTS_DIR, self.path.replace("://", "/").lstrip("/")))

    def logfile(self):
        return luigi.contrib.opener.OpenerTarget("%s.%s.log.txt" % (self.path, self.hostname))
            
    def output(self):
        return luigi.local_target.LocalTarget(
            os.path.join(ENVIRONMENTS_DIR, self.path.replace("://", "/").lstrip("/"), "done"))

def get_scheduler_url(task):
    return task.set_progress_percentage.__self__._scheduler._url
    
class RunTask(poltergust_luigi_utils.logging_task.LoggingTask, luigi.Task):
    path = luigi.Parameter()
    hostname = luigi.Parameter(significant=False, visibility=luigi.parameter.ParameterVisibility.HIDDEN)
    retry_on_error = luigi.Parameter(default=False)
    
    @property
    def scheduler(self):
        return self.set_progress_percentage.__self__._scheduler

    @property
    def scheduler_url(self):
        return get_scheduler_url(self)
    
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
                self.log("Task failed: %s" % e)
                self.log(traceback.format_exc())
            
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

def list_wildcard(fs, path):
    if hasattr(fs, "list_wildcard"):
        return fs.list_wildcard(path)
    else:
        dirpath, pattern = path.rsplit("/", 1)
        return fnmatch.filter(fs.listdir(dirpath), path) 
        
class RunTasks(luigi.Task):
    path = luigi.Parameter()
    hostname = luigi.Parameter()

    @property
    def scheduler_url(self):
        return get_scheduler_url(self)

    def run(self):
        while True:
            tasks = [RunTask(path=path.replace(".config.yaml", ""), hostname=self.hostname)
                   for path in list_wildcard(self.output().fs, '%s/*.config.yaml' % (self.path,))]
            print("=============================================")
            print(tasks)
            for task in tasks:
                res = luigi.build([task], scheduler_url=self.scheduler_url, detailed_summary=True)
                print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
                print(res.summary_text)
                print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
            time.sleep(1)
    
    def output(self):
        # This target should never actually be created...
        return luigi.contrib.opener.OpenerTarget('%s/done' % (self.path,))
    


import random
class TestTask(luigi.Task):
    name = luigi.Parameter()
    time = luigi.Parameter()
    
    def run(self):
        t = int(self.time)
        for x in range(t):
            self.set_progress_percentage(100 * x / t)
            time.sleep(1)
            print("¯\_( ͡° ͜ʖ ͡°)_/¯\n")
        
        with self.output().open("w") as f:
            f.write("DONE")        

    def output(self):
        return luigi.contrib.opener.OpenerTarget(self.name)



# luigi --module emerald_algorithms_evaluation.luigi Pipeline --param-evaluation_name=test-luigi-redhog-1
