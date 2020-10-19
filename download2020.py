# coding:utf-8
import csv
import subprocess
from datetime import datetime
import os
import pandas as pd
import sys
import threadpool
import pexpect

API_KEY = "fa08a4ad8d8c9d3c56236d27bd9b99bb83c66c3fd65642d496ea2cbd13d4e8a4"
MARKET = "play.google.com"
DOWNLOAD_PATH = "/data/sdc/yanjie/APK2020"
LATESTCSV = "/data/sdc/yanjie/latest.csv"
# CHECK_PATH = "/mnt/fit-Knowledgezoo-vault/vault/apks/"
passwd = "changeme"
script_filename = "download_log"
fout = open(script_filename, "wb")
save_list = "SELECTED_1w.txt"


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

    def process_one(self, args):
        SHA256 = args
        try:
            if os.path.exists(os.path.join(DOWNLOAD_PATH, SHA256 + ".apk")):
                return

            curl = "sudo curl -O --remote-header-name -G -d apikey=" + API_KEY + " -d sha256=" + SHA256 + " \
                                        https://androzoo.uni.lu/api/download"
            os.chdir(DOWNLOAD_PATH)
            child = pexpect.spawn(curl)
            child.logfile = fout
            # child.logfile = None
            index = child.expect(['password', pexpect.EOF, pexpect.TIMEOUT])
            if index == 0:
                child.sendline(passwd)
                downloadindex = child.expect(['Saved', pexpect.EOF], timeout=600)
                if downloadindex == 0:
                    print(SHA256 + " downloaded!")
                elif downloadindex == 1:
                    print(SHA256 + " EOF!")
                else:
                    print("Time out!")
            elif index == 1:
                print("EOF!")
            else:
                print("Time out!")
            child.close(force=True)
            return

        except Exception as e:
            print(e, SHA256)
            return None

    def process(self, selected_sha256):
        files = selected_sha256
        args = [file for file in files]
        pool = threadpool.ThreadPool(self.max_jobs)
        requests = threadpool.makeRequests(self.process_one, args)
        [pool.putRequest(req) for req in requests]
        pool.wait()

    def start(self):
        selected_sha256 = []
        if os.path.exists(save_list):
            with open(save_list, "r") as fr:
                content = fr.read().split("\n")
                selected_sha256 = [i.strip() for i in content if i]
        else:
            pkgs = {}
            selected_num = 0
            df = pd.read_csv(LATESTCSV, header=0, chunksize=10000)
            for chunk in df:
                for i in range(len(chunk)):
                    if selected_num == 10000:
                        break
                    HEADERS = chunk.columns.values.tolist()
                    k = chunk.iloc[i]['pkg_name']
                    m = chunk.iloc[i]['markets']
                    if k in pkgs or MARKET not in m:
                        continue
                    pkgs[k] = 1

                    dex_date = chunk.iloc[i]['dex_date']
                    line = chunk.iloc[i].tolist()
                    if "-" and ":" not in str(dex_date):
                        continue
                    date = datetime.strptime(str(dex_date), '%Y-%m-%d %H:%M:%S')
                    i_year = date.year
                    i_month = date.month
                    i_day = date.day
                    i_time = date.time()
                    if i_year < 2020:
                        continue
                    selected_sha256.append(chunk.iloc[i]['sha256'])
                    selected_num += 1

            print(len(selected_sha256))
            with open(save_list, "w") as fw:
                for item in selected_sha256:
                    fw.write(item)
                    fw.write('\n')

        self.process(selected_sha256)


if __name__ == '__main__':
    if not os.path.exists(DOWNLOAD_PATH):
        os.mkdir(DOWNLOAD_PATH)
    analysis = Analysis()
    analysis.start()
