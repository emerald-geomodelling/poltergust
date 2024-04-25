import os
import re
import os.path
from pathlib import Path
import zipfile
import luigi
import luigi.contrib.opener
import luigi.local_target
import pieshell
import datetime
import psutil
import signal

DB_URL = os.environ.get("DB_URL")
ENVIRONMENTS_DIR = os.environ.get("ENVIRONMENTS_DIR", "/tmp/environments")
DOWNLOAD_ENVIRONMENT = os.environ.get("DOWNLOAD_ENVIRONMENT", False)
UPLOAD_ENVIRONMENT = os.environ.get("UPLOAD_ENVIRONMENT", False)
TAG = r"{{POLTERGUST_PIP}}"
PIPELINE_URL = os.environ.get("PIPELINE_URL")


def strnow():
    return datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S.%f")

def make_environment(envpath, environment, log):
    _ = pieshell.env(exports=dict(pieshell.env._exports))
    if not os.path.exists(envpath):
        envdir = os.path.dirname(envpath)
        if not os.path.exists(envdir):
            os.makedirs(envdir)
        +_.virtualenv(envpath, **environment.get("virtualenv", {}))
    +_.bashsource(envpath + "/bin/activate")
    for dep in environment["dependencies"]:
        if TAG in dep:
            dep = re.sub(TAG, _._exports["GITHUB_TOKEN_EMRLD"], dep)
        for line in _.pip.install(dep):
            log(line)


def zip_environment(envpath, log):
    """
    Create a zip compression level 6 archive of a python virtualenv
    Note that zipping virtualenvs is not recommended according to PEP405:
        "Not considered as movable or copyable – you just recreate the 
        same environment in the target location"

    However, this will work if the two execution environments are "exactly"
    the same

    @Args:
        envpath: local path to the virtualenv directory
    @Returns:
        archive: name of the zipped archive
    """
    archive = envpath + ".zip"
    envdir = Path(envpath)
    with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=6) as f:
        for file in envdir.rglob('*'):
            f.write(file, arcname=file.relative_to(envdir))
    log("Zipped: {}".format(archive))
    return archive


def download_environment(envpath, path, log):
    """
    Download a python virtualenv and extract it
    @Args
        envpath: local path to the virtualenv directory
        path: path to downloadable virtualenv (GCS/local disk)
        log: luigi logger
    """
    if not os.path.exists(envpath):
        envdir = os.path.dirname(envpath)
        if not os.path.exists(envdir):
            os.makedirs(envdir)

    fs = luigi.contrib.opener.OpenerTarget(path).fs
    with fs.download(path) as z:
        with zipfile.ZipFile(z, mode="r") as arc:
            arc.extractall(envpath)


def kill_proc_tree(pid, sig=signal.SIGTERM, timeout=5):
    parent = psutil.Process(pid)
    children = parent.children(recursive=True)
    # children.append(parent)

    parent.terminate()
    try:
        parent.wait(timeout)
    except psutil.TimeoutExpired:
        parent.kill()

    for proc in children:
        try:
            proc.send_signal(sig)
        except psutil.NoSuchProcess:
            pass
    gone, alive = psutil.wait_procs(children, timeout=2)
    for p in alive:
        p.kill()

    return gone


def remove_config_from_pipeline(cfg):
    cfg_src = cfg + ".config.yaml"
    cfg_dst = cfg + ".done.yaml"

    fs = luigi.contrib.opener.OpenerTarget(cfg_src).fs
    if PIPELINE_URL:
        full_cfg_path = PIPELINE_URL + "/" + cfg_src
        if fs.exists(full_cfg_path):
            try:
                fs.move(full_cfg_path, cfg_dst)
            except:
                pass
            finally:
                return f"{full_cfg_path} moved!"
        else:
            return f"not exists!: {full_cfg_path}"
    return "PIPELINE_URL not found!"


