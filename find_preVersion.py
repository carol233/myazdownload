# coding:utf-8
import os
import pandas as pd
import subprocess
import sys
import threadpool
import pexpect
import re

API_KEY = "fa08a4ad8d8c9d3c56236d27bd9b99bb83c66c3fd65642d496ea2cbd13d4e8a4"

DOWNLOAD_PATH = "/data/sdc/yanjie/AndroidDL/LineageAll"
CSV_FOLDER = "/data/sdc/yanjie/androzoo/lineages/year_folder"
CSV_FOLDER2 = "/data/sdc/yanjie/androzoo/lineages/month_folder"
CSV_FOLDER3 = "/data/sdc/yanjie/androzoo/lineages/csv_folder"
APK_FOLDER = "/data/sdc/yanjie/AndroidDL/APK"
# CHECK_PATH = "/mnt/fit-Knowledgezoo-vault/vault/apks/"
script_filename = "download_log_all"
fout = open(script_filename, "wb")


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
        self.max_jobs = 10


    def get_package(self, apkfile):
        pkgName = ""
        try:
            AAPT_CMD = "aapt dump badging " + apkfile + "| grep package:\ name"
            output = subprocess.check_output(AAPT_CMD, shell=True, timeout=20)
        except subprocess.TimeoutExpired as exc:
            print("Command timed out: {}".format(exc))
            return
        except subprocess.CalledProcessError as e:
            output = e.output  # Output generated before error
            code = e.returncode  # Return code
            return

        aapt_output = output.decode('utf-8')
        """ EXAMPLE
        package: name='com.example.myapplication' versionCode='1' 
        versionName='1.0' compileSdkVersion='29' compileSdkVersionCodename='10'
        """

        pkg = re.findall(r"package: name='([0-9a-zA-Z.]+)'", aapt_output)
        if pkg:
            pkgName = pkg[0].strip()

        print("pkg:", pkgName)
        return pkgName

    def process_one(self, args):
        apk = args
        pkg_name = self.get_package(apk)
        if not pkg_name:
            # extract pkg name error
            return

        csvfile = os.path.join(CSV_FOLDER3, pkg_name + ".csv")
        if not os.path.exists(csvfile):
            # csvfile = os.path.join(CSV_FOLDER2, pkg_name + ".csv")
            # if not os.path.exists(csvfile):
            print("no csv file!")
            return

        DOWNLOAD_PATH_2 = os.path.join(DOWNLOAD_PATH, pkg_name)

        with open(csvfile, 'r') as f:
            try:
                df = pd.read_csv(f, header=0)
                all_lines_num = len(df)
                print(pkg_name, all_lines_num)
                for i in range(all_lines_num):

                    SHA256 = df.iloc[i]['sha256']
                    if not os.path.exists(DOWNLOAD_PATH_2):
                        os.mkdir(DOWNLOAD_PATH_2)
                    if os.path.exists(os.path.join(DOWNLOAD_PATH_2, SHA256 + ".apk")):
                        print("apk already exits!")
                        continue

                    # "sudo curl -O --remote-header-name -G -d apikey=fa08a4ad8d8c9d3c56236d27bd9b99bb83c66c3fd65642d496ea2cbd13d4e8a4 -d sha256=FC6A87C7E9CB83DED48F099B6041B12AC9C6972776E73E23F98FC83F9D90148C https://androzoo.uni.lu/api/download"
                    curl = "curl -O --remote-header-name -G -d apikey=" + API_KEY + " -d sha256=" + SHA256 + " \
                                    https://androzoo.uni.lu/api/download"

                    os.chdir(DOWNLOAD_PATH_2)

                    child = pexpect.spawn(curl)
                    child.logfile = fout
                    index = child.expect(['Saved', pexpect.EOF], timeout=600)
                    if index == 0:
                        print("Success!")
                    elif index == 1:
                        print("EOF!")
                    else:
                        print("Time out!")
                    child.close(force=True)

            except Exception as e:
                print(e, apk)
                return None

    def row_count(self, filename):
        with open(filename) as in_file:
            return sum(1 for _ in in_file)

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
    if not os.path.exists(DOWNLOAD_PATH):
        os.mkdir(DOWNLOAD_PATH)
    analysis = Analysis()
    analysis.start()
    fout.close()
