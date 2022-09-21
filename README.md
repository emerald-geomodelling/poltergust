# Poltergust

Trigger [Luigi](https://luigi.readthedocs.io/en/stable/) tasks on multiple worker
machines. Python modules for tasks and their dependencies are
installed automatically in virtualenvs on each worker.

# Files used by the Poltergust task runner

`gs://mybucket/pipeline/mypipeline.config.yaml`:
```
environment: gs://mybucket/environment/myenv.yaml

task: SomeTask
module: some_luigi_pipeline_module
some-task-argument: some-value
```

`gs://mybucket/environment/myenv.yaml`:
```
virtualenv:
  python: python3.8
dependencies:
  - pandas
  - matplotlib
```

An environment file specifies a virtualenv to be created, with the
arguments specified (--python=python3.8), and a set of dependencies to
be installed using pip. Each dependency will be installed inside the
virtualenv with pip install, using the verbatim dependency string
given.

A task file specifies an environment to run the task in, a luigi root
task name, and any other arguments to give to the luigi command (with
`--` removed). Note: You probably want to specify the same
`scheduler-url` here as you did when running the Poltergust task
runner.

When a task is done, the task config file will be moved from

`gs://mybucket/pipeline/mypipeline.config.yaml`

to

`gs://mybucket/pipeline/mypipeline.done.yaml`

# Instantiating the task runner

luigi RunTasks --module poltergust --path=gs://mybucket/pipeline
