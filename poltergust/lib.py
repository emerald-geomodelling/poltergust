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
import fnmatch


class Config:
    DB_URL = os.environ.get("DB_URL")
    ENVIRONMENTS_DIR = os.environ.get("ENVIRONMENTS_DIR", "/tmp/environments")
    DOWNLOAD_ENVIRONMENT = os.environ.get("DOWNLOAD_ENVIRONMENT", False)
    UPLOAD_ENVIRONMENT = os.environ.get("UPLOAD_ENVIRONMENT", False)
    PIPELINE_URL = os.environ.get("PIPELINE_URL", "")
    TAG = r"{{POLTERGUST_PIP}}"


def strnow():
    return datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S.%f")


def makedirs_recursive(envpath):
    if not os.path.exists(envpath):
        envdir = os.path.dirname(envpath)
        if not os.path.exists(envdir):
            os.makedirs(envdir)
    return envpath


def list_wildcard(fs, path):
    if hasattr(fs, "list_wildcard"):
        return fs.list_wildcard(path)
    else:
        dirpath, pattern = path.rsplit("/", 1)
        return fnmatch.filter(fs.listdir(dirpath), path) 


def make_environment(envpath, environment, log):
    _ = pieshell.env(exports=dict(pieshell.env._exports))
    # if not os.path.exists(envpath):
    #     envdir = os.path.dirname(envpath)
    #     if not os.path.exists(envdir):
    #         os.makedirs(envdir)
    makedirs_recursive(envpath)
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
    makedirs_recursive(envpath)
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


def rename_file(src, dst):
    fs = luigi.contrib.opener.OpenerTarget(src).fs
    if fs.exists(src):
        try:
            fs.move(src, dst)
        except:
            pass
        finally:
            return f"{src} moved to {dst}"
    else:
        return f"not exists!: {src}"


def remove_config_from_pipeline(cfg, dst=None, gcs=True):
    cfg_src = cfg + ".config.yaml"
    cfg_dst = cfg + ".done.yaml"

    if not Config.PIPELINE_URL.startswith("gs://"):
        return f"GCS PIPELINE_URL not found!, {Config.PIPELINE_URL}"

    full_cfg_path = Config.PIPELINE_URL + "/" + cfg_src
    full_dst_path = Config.PIPELINE_URL + "/" + cfg_dst
    ret = rename_file(full_cfg_path, full_dst_path)
    return ret
    # ret = rename_file(cfg, dst)
    # return ret

