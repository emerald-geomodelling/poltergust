import os
import yaml
import sys
import traceback
import luigi
import luigi.contrib.gcs
import luigi.format
import luigi.mock
import pieshell

def make_environment(envpath, environment):
    _ = pieshell.env(exports=dict(pieshell.env._exports))
    if not os.path.exists(envpath):
        +_.virtualenv(envpath, **environment.get("virtualenv", {}))
    +_.bashsource(envpath + "/bin/activate")
    for dep in environment["dependencies"]:
        +_.pip.install(dep)

class RunTask(luigi.Task):
    path = luigi.Parameter()

    def run(self):
        with luigi.contrib.gcs.GCSTarget('%s.config.yaml' % (self.path,)).open("r") as f:
            task = yaml.load(f, Loader=yaml.SafeLoader)
            
        with luigi.contrib.gcs.GCSTarget(task["environment"]).open("r") as f:
            environment = yaml.load(f, Loader=yaml.SafeLoader)
            
        envpath = os.path.join("/tmp/environments", task["environment"].replace("://", "/"))
        make_environment(envpath, environment)

        scope = pieshell.environ.EnvScope(env=pieshell.env(envpath, interactive=True))
        
        task_args = dict(task)
        task_args.pop("environment", None)
        command = task_args.pop("command", None)
        task_name = task_args.pop("task", None)

        scope["task_name"] = task_name
        scope["task_args"] = task_args
        
        if command is None:
            command = "+luigi(task_name, **task_args)"
        
        eval(command, scope)

        self.output().fs.move('%s.config.yaml' % (self.path,), '%s.done.yaml' % (self.path,))

    def output(self):
         return luigi.contrib.gcs.GCSTarget(
             '%s.done.yaml' % (self.path,))
        
class RunTasks(luigi.Task):
    path = luigi.Parameter()
    
    def run(self):
        for path in self.output().fs.list_wildcard('%s/*.config.yaml' % (self.path,)):
            yield RunTask(path.replace(".config.yaml", ""))
    
    def output(self):
        # This target should never actually be created...
        return luigi.contrib.gcs.GCSTarget('%s/done' % (self.path,))
    

# luigi --module emerald_algorithms_evaluation.luigi Pipeline --param-evaluation_name=test-luigi-redhog-1
