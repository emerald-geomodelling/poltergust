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
import time

def make_environment(envpath, environment):
    _ = pieshell.env(exports=dict(pieshell.env._exports))
    if not os.path.exists(envpath):
        envdir = os.path.dirname(envpath)
        if not os.path.exists(envdir):
            os.makedirs(envdir)
        +_.virtualenv(envpath, **environment.get("virtualenv", {}))
    +_.bashsource(envpath + "/bin/activate")
    for dep in environment["dependencies"]:
        +_.pip.install(dep)

class MakeEnvironment(luigi.Task):
    path = luigi.Parameter()
    hostname = luigi.Parameter()

    def run(self):
        with luigi.contrib.gcs.GCSTarget(self.path).open("r") as f:
            environment = yaml.load(f, Loader=yaml.SafeLoader)        
        make_environment(self.output().path, environment)
        
    def output(self):
        return luigi.local_target.LocalTarget(
            os.path.join("/tmp/environments", self.path.replace("://", "/")))
        
class RunTask(luigi.Task):
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

        envpath = yield MakeEnvironment(path=task["environment"], hostname=self.hostname)
        envpath = envpath.path

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
            command = "+luigi(task_name, **task_args)"

        scope = pieshell.environ.EnvScope(env=_)
        scope["task_name"] = task_name
        scope["task_args"] = task_args

        # Rerun the task until it is actually being run (or is done!)
        # We need to do this, since luigi might exit because all
        # dependencies of a task are already being run by other
        # workers, and our task can't even start...
        while True:
            try:
                eval(command, scope)
            except pieshell.PipelineFailed as e:
                print(e)
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
