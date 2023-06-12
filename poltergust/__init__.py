import os.path
import yaml
import sys
import traceback
import luigi
import luigi.contrib.gcs
import luigi.format
import luigi.mock
import luigi.local_target
import pieshell
import traceback
import math
import time

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

def iter_to_pcnt(x, a=0.5):
    return 100 * a * math.log(1 + x) / (1 + a*math.log(1 + x))

class Logging(object):
    def on_failure(self, exception):
        traceback_string = traceback.format_exc()
        msg = "Runtime error:\n%s" % traceback_string
        self._log(msg)
        self.close_log()
        return msg
        
    def on_success(self):
        self._log("DONE")
        self.close_log()
        
    def close_log(self):
        with self.logfile().open("w") as f:
            f.write("\n".join(self.msgs) + "\n")
    
    def _log(self, msg):
        if not hasattr(self, "msgs"):
            self.msgs = []
        print(msg)
        self.msgs.append(msg)

    def log(self, msg):
        self._log(msg)
        self.set_status_message("\n".join(self.msgs))
        self.set_progress_percentage(iter_to_pcnt(len(self.msgs)))

        
class MakeEnvironment(Logging, luigi.Task):
    path = luigi.Parameter()
    hostname = luigi.Parameter()
        
    def run(self):
        with luigi.contrib.gcs.GCSTarget(self.path).open("r") as f:
            environment = yaml.load(f, Loader=yaml.SafeLoader)
        make_environment(self.envdir().path, environment, self.log)
        with self.output().open("w") as f:
            f.write("DONE")        
        
    def envdir(self):
        return luigi.local_target.LocalTarget(
            os.path.join("/tmp/environments", self.path.replace("://", "/")))
    
    def logfile(self):
        return luigi.contrib.gcs.GCSTarget("%s.%s.log.txt" % (self.path, self.hostname))
            
    def output(self):
        return luigi.local_target.LocalTarget(
            os.path.join("/tmp/environments", self.path.replace("://", "/"), "done"))
        
class RunTask(Logging, luigi.Task):
    path = luigi.Parameter()
    hostname = luigi.Parameter()
    
    @property
    def scheduler(self):
        return self.set_progress_percentage.__self__._scheduler

    @property
    def scheduler_url(self):
        return self.scheduler._url
    
    def run(self):
        cfg = luigi.contrib.gcs.GCSTarget('%s.config.yaml' % (self.path,))
        try:
            with cfg.open("r") as f:
                task = yaml.load(f, Loader=yaml.SafeLoader)
        except:
            if not cfg.fs.exists(cfg.path):
                # The task has already been marked as done by another worker.
                return
            raise

        with luigi.contrib.gcs.GCSTarget(task["environment"]).open("r") as f:
            environment = yaml.load(f, Loader=yaml.SafeLoader)        

        env = MakeEnvironment(path=task["environment"], hostname=self.hostname)
        yield env
        envpath = env.envdir().path

        _ = pieshell.env(envpath, interactive=True)
        +_.bashsource(envpath + "/bin/activate")

        _._exports.update(environment.get("variables", {}))
        _._exports.update(task.get("variables", {}))

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
                
        fs = self.output().fs
        src = '%s.config.yaml' % (self.path,)
        dst = '%s.done.yaml' % (self.path,)
        try:
            fs.move(src, dst)
        except:
            # Might already have been moved by another node...
            pass

    def logfile(self):
        return luigi.contrib.gcs.GCSTarget("%s.%s.log.txt" % (self.path, self.hostname))

    def output(self):
         return luigi.contrib.gcs.GCSTarget(
             '%s.done.yaml' % (self.path,))

        
class RunTasks(luigi.Task):
    path = luigi.Parameter()
    hostname = luigi.Parameter()

    def requires(self):
        return [RunTask(path=path.replace(".config.yaml", ""), hostname=self.hostname)
                for path in self.output().fs.list_wildcard('%s/*.config.yaml' % (self.path,))]

    def run(self):
        pass
    
    def output(self):
        # This target should never actually be created...
        return luigi.contrib.gcs.GCSTarget('%s/done' % (self.path,))
    

# luigi --module emerald_algorithms_evaluation.luigi Pipeline --param-evaluation_name=test-luigi-redhog-1
