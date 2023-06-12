import luigi
import time

class Xy(luigi.Task):
    def run(self):
        time.sleep(10000000)
    
    def output(self):
        return luigi.local_target.LocalTarget("/tmp/foo")

