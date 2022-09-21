import os
import yaml
import sys
import traceback
import luigi
import luigi.contrib.gcs
import luigi.format
import luigi.mock
import pieshell

"""
Files used by the task runner:

--{gs://mybucket/pipeline/mypipeline/config.yaml}--
environment: gs://mybucket/environment/myenv.yaml
command: +luigi("--module=some_luigi_pipeline_module", "SomeTask")
--{end of file}--

--{gs://mybucket/environment/myenv.yaml}--
virtualenv:
  python: python3.8
dependencies:
  - pandas
  - matplotlib
--{end of file}--

An environment file specifies a virtualenv to be created, with the
arguments specified (--python=python3.8), and a set of dependencies to
be installed using pip. Each dependency will be installed inside the
virtualenv with pip install, using the verbatim dependency string
given.
"""


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
        with luigi.contrib.gcs.GCSTarget('%s/config.yaml' % (self.path,)).open("r") as f:
            task = yaml.load(f, Loader=yaml.SafeLoader)
            
        with luigi.contrib.gcs.GCSTarget(task["environment"]).open("r") as f:
            environment = yaml.load(f, Loader=yaml.SafeLoader)
            
        envpath = os.path.join("/tmp/environments", task["environment"].replace("://", "/"))
        make_environment(envpath, environment)

        eval(task["command"], pieshell.environ.EnvScope(env=pieshell.env(envpath, interactive=True)))
            
        with self.output().open("w") as f:
            f.write("DONE")

    def output(self):
         return luigi.contrib.gcs.GCSTarget(
             '%s/status.done' % (self.path,),
             format=None, client=None)
        
class RunTasks(luigi.Task):
    path = luigi.Parameter()
    
    def run(self):
        c = luigi.contrib.gcs.GCSClient()
        for path in c.list_wildcard('%s/*' % (self.path,)):
            if path.endswith("/config.yaml"):
                path = path.rsplit("/", 1)[0]
                if not c.exists(path + "/status.done"):
                    yield RunTask(path)
    
    def output(self):
        return luigi.mock.MockTarget('/dummy')
    

# luigi --module emerald_algorithms_evaluation.luigi Pipeline --param-evaluation_name=test-luigi-redhog-1
