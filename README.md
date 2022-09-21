# Poltergust

Trigger [Luigi](https://luigi.readthedocs.io/en/stable/) tasks on multiple worker
machines. Python modules for tasks and their dependencies are
installed automatically in virtualenvs on each worker.

Files used by the task runner:

`gs://mybucket/pipeline/mypipeline/config.yaml`:
```
environment: gs://mybucket/environment/myenv.yaml
command: +luigi("--module=some_luigi_pipeline_module", "SomeTask")
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
