import os
import shutil
import shlex
import pandas as pd
import sys
import threadpool
import threading
from subprocess import Popen, PIPE
from threading import Timer

JAR_PATH = "SDKUpdateMining.jar"
APK_FOLDER = "/data/sdc/yanjie/APK2020"
SAVEJIMPLE_DIR = "/data/sdc/yanjie/SAVEJIMPLE"
SAVECSV_DIR = "/data/sdc/yanjie/SAVECSV"
Android_jar = "/home/yanjie/android-sdk-linux/platforms"
RECORD_TXT = "record.txt"
all_solved = {}


def getFileList(rootDir, pickstr):
    """
    :param rootDir:  root directory of dataset
    :return: A filepath list of sample
    """
    filePath = []
    for parent, dirnames, filenames in os.walk(rootDir):
        for filename in filenames:
            if filename.endswith(pickstr):
                file = os.path.join(parent, filename)
                filePath.append(file)
    return filePath


class Analysis:
    def __init__(self):
        self.max_jobs = 15
        self.lock = threading.Lock()

    def run(self, cmd, timeout_sec):
        proc = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
        timer = Timer(timeout_sec, proc.kill)
        try:
            timer.start()
            stdout, stderr = proc.communicate()
        finally:
            timer.cancel()

    def process_one(self, args):
        file = args
        sha256 = os.path.split(file)[-1][:-4]
        if sha256 in all_solved:
            return
        output_dir = os.path.join(SAVEJIMPLE_DIR, sha256)
        output_file = os.path.join(SAVECSV_DIR, sha256 + ".csv")

        try:
            self.lock.acquire()
            with open(RECORD_TXT, "a+") as fw:
                fw.write(sha256)
                fw.write("\n")
            self.lock.release()

            print("[+] PreSolving " + sha256)
            CMD = "java -Xms1024m -Xmx4096m -XX:MaxNewSize=2048m " \
                  "-jar " + JAR_PATH + " " + file + " " + Android_jar + " " + output_file + " " + output_dir
            self.run(CMD, 30)

        except Exception as e:
            print(e, sha256)

        count = len(open(output_file, 'rU').readlines())
        if count == 1:
            if os.path.isfile(output_file):
                os.remove(output_file)
            if os.path.exists(output_dir):
                shutil.rmtree(output_dir)
        return

    def process(self):
        files = getFileList(APK_FOLDER, ".apk")
        args = [file for file in files]
        pool = threadpool.ThreadPool(self.max_jobs)
        requests = threadpool.makeRequests(self.process_one, args)
        [pool.putRequest(req) for req in requests]
        pool.wait()

    def start(self):
        self.process()


if __name__ == '__main__':
    if os.path.exists(RECORD_TXT):
        with open(RECORD_TXT, "r") as fr:
            solved = fr.read().split("\n")
            for item in solved:
                all_solved[item] = 1

    if not os.path.exists(SAVEJIMPLE_DIR):
        os.mkdir(SAVEJIMPLE_DIR)
    if not os.path.exists(SAVECSV_DIR):
        os.mkdir(SAVECSV_DIR)
    analysis = Analysis()
    analysis.start()

