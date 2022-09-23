# Poltergust

Trigger [Luigi](https://luigi.readthedocs.io/en/stable/) tasks on multiple worker
machines. Python modules for tasks and their dependencies are
installed automatically in virtualenvs on each worker.

## Motivation & design

The [Luigi documentation](https://luigi.readthedocs.io/en/stable/execution_model.html#workers-and-task-execution) states that

> The most important aspect is that no execution is transferred. When you run a Luigi workflow, the worker schedules all tasks,
> and also executes the tasks within the process.

> The benefit of this scheme is that it’s super easy to debug since all execution takes place in the process. It also makes
> deployment a non-event. During development, you typically run the Luigi workflow from the command line, whereas when you deploy it,
> you can trigger it using crontab or any other scheduler.

However, in practice this is what makes deployment *hard* since you have to figure ut a way to install dependencies and code and to trigger your tasks on your worker nodes somehow. For non repeating pipelines (daily etc), this becomes increasingly complex.

Poltergust takes care of this for you! You run the poltergust main task on a set of machines (restarting it as soon as it finishes), and can then submit tasks to be run using files. These tasks consists of a luigi task to be run as well as the code to run it and any dependencies to be installed.

## Limitations and requirements

Poltergust currently only supports GCSTargets (and thus gs:// url:s) for its files and so requires Google Cloud Storage. 

## Files used by the Poltergust task runner

`gs://mybucket/pipeline/mypipeline.config.yaml`:
```
environment: gs://mybucket/environment/myenv.yaml

task:
  name: SomeTask
  module: some_luigi_pipeline_module
  some-task-argument: some-value

variables:
  DB_HOST: database.com
```

`gs://mybucket/environment/myenv.yaml`:
```
virtualenv:
  python: python3.8

dependencies:
  - pandas
  - matplotlib
  
variables:
  PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION: python
```

An environment file specifies a virtualenv to be created, with the
arguments specified (--python=python3.8), and a set of dependencies to
be installed using pip. Each dependency will be installed inside the
virtualenv with pip install, using the verbatim dependency string
given. It can optionally also specify environment variables to be set
using the key `variables`.

A task file specifies an environment to run the task in, a luigi root
task name, and any other arguments to give to the luigi command (with
`--` removed). Note: `--scheduler-url` will be automatically added. It
can optionally also specify environment variables to be set using the
key `variables`.

When a task is done `gs://mybucket/pipeline/mypipeline.config.yaml` is
renamed to `gs://mybucket/pipeline/mypipeline.done.yaml` (since a task
is run on multiple nodes: the first one to mark the task as done renames
the file).

## Instantiating the task runner manually on a single machine

```
while true; do
  luigi RunTasks --module poltergust --path=gs://mybucket/pipeline
  sleep 1
done
```

## Creating a cluster

### Google DataProc

To create a cluster with 2 nodes and the name `mycluster`:
```
cd clusters
gsutil mb gs://mycluster
./dataproc-create-cluster.sh mycluster 2
```

The above will open an ssh connection to the master node after creating the cluster, forwarding port 8082, so that you can view the cluster status
in a browser at http://localhost:8082

