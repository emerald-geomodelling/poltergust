import os.path
import yaml
import sys
import traceback
import luigi
import luigi.contrib.gcs
import luigi.format
import luigi.mock
import pieshell
import socket

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
        with luigi.contrib.gcs.GCSTarget('%s.config.yaml' % (self.path,)).open("r") as f:
            task = yaml.load(f, Loader=yaml.SafeLoader)
            
        with luigi.contrib.gcs.GCSTarget(task["environment"]).open("r") as f:
            environment = yaml.load(f, Loader=yaml.SafeLoader)
            
        envpath = os.path.join("/tmp/environments", task["environment"].replace("://", "/"))
        make_environment(envpath, environment)

        _ = pieshell.env(envpath, interactive=True)
        +_.bashsource(envpath + "/bin/activate")

        _._exports.update(environment.get("variables", {}))
        _._exports.update(task.get("variables", {}))
        
        command = task.get("command", None)

        task_args = dict(task.get("task", {}))
        task_name = task_args.pop("name", None)
        task_args["scheduler-url"] = self.scheduler_url
        
        if command is None:
            command = "+luigi(task_name, **task_args)"

        scope = pieshell.environ.EnvScope(env=_)
        scope["task_name"] = task_name
        scope["task_args"] = task_args

        eval(command, scope)

        with self.output().open("w") as f:
            f.write("DONE")

    def output(self):
         return luigi.contrib.gcs.GCSTarget(
             '%s.%s.done' % (self.path, self.hostname))
        
class RunTasks(luigi.Task):
    path = luigi.Parameter()

    def requires(self):
        return [RunTask(path=path.replace(".config.yaml", ""), hostname=socket.gethostname())
                for path in self.output().fs.list_wildcard('%s/*.config.yaml' % (self.path,))]

    def run(self):
        pass
    
    def output(self):
        # This target should never actually be created...
        return luigi.contrib.gcs.GCSTarget('%s/done' % (self.path,))
    

# luigi --module emerald_algorithms_evaluation.luigi Pipeline --param-evaluation_name=test-luigi-redhog-1
