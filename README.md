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

However, in practice this is what makes deployment *hard* since you have to figure out a way to install dependencies and code and to trigger your tasks on your worker nodes somehow. For non repeating pipelines (daily etc), this becomes increasingly complex.

Poltergust takes care of this for you! You run the poltergust main task on a set of machines (restarting it as soon as it finishes), and can then submit tasks to be run using files. These tasks consists of a luigi task to be run as well as the code to run it and any dependencies to be installed.

## Limitations and requirements

Poltergust supports all target types supported by
luigi.contrib.opener, plus Google Cloud Storage targets (gs:// urls) for file storage.

## Running

Docker compose file to start poltergust and luigi:

```
version: "3.9"
services:
  poltergust:
#    build: .
    image: redhogorg/poltergust:0.0.2
    volumes:
      - ~/.config/gcloud:/root/.config/gcloud
    environment:
      - PIPELINE_URL=gs://mybucket/pipeline
      - SCHEDULER_URL=http://scheduler:8082
    deploy:
      replicas: 1 # Set this to how many workers you want

  scheduler:
    image: spotify/luigi
    ports:
      - 8082:8082
```

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
is run on multiple nodes: the first one to mark the task as done
renames the file). If the task fails, the config file is instead
renamed to `gs://mybucket/pipeline/mypipeline.error.yaml`

## Instantiating the task runner manually on a single machine

```
while true; do
  luigi RunTasks --module poltergust --hostname="$(hostname -f)" --path=gs://mybucket/pipeline
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

### Docker stack

* Build a docker image using `docker built --tag poltergust:0.0.1`
* Modify the `docker-compose.yml` to set number of replicas and GCS bucket.
* Deploy the docker stack using `docker stack deply poltergust -c docker-compose.yml`


# Tips for paralellization

When writing your Luigi pipelines, it is sometimes necessary to yield tasks form the `run` method. This can limit paralellization if done wrongly.
To make sure your pipeline works optimally, do not yeild a list of tasks, instead, yield a single task that in turn has those tasks returned by its `require` method:

Replace:
```
class MainTask(luigi.Task):
    def run(self):
         ....
         listofnumbers = makeList()
         yield [SubTask(number) for number in listofnumbers]
```

With:
```
class MiddleTask(luigi.Task):
    listofnumbers = luigi.Parameter()
    def run(self):
        return [SubTask(number) for number in self.listofnumbers]
    ....

class MainTask(luigi.Task):
    def run(self):
         ....
         listofnumbers = makeList()
         yield MiddleTask(listofnumbers=listofnumbers)
```



