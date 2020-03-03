# coding:utf-8
import os
import pandas as pd
import sys
import threadpool
import pexpect

API_KEY = "fa08a4ad8d8c9d3c56236d27bd9b99bb83c66c3fd65642d496ea2cbd13d4e8a4"

DOWNLOAD_PATH = "/home/yanjie/azdownload/OSS_APK"
CSV_FOLDER = "/home/yanjie/azdownload/AndroZooGithub"
passwd = "changeme"
script_filename = "download_log"
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
        self.max_jobs = 15

    def process_one(self, args):
        file = args
        with open(file, 'r') as f:
            try:
                df = pd.read_csv(f, header=0)
                for i in range(len(df)):
                    SHA256 = df.iloc[i]['sha256']
                    if_the_latest_version = df.iloc[i]['if_the_latest_version']
                    if if_the_latest_version != "Yes":
                        continue
                    if os.path.exists(os.path.join(DOWNLOAD_PATH, SHA256 + ".apk")):
                        continue

                    # "sudo curl -O --remote-header-name -G -d apikey=fa08a4ad8d8c9d3c56236d27bd9b99bb83c66c3fd65642d496ea2cbd13d4e8a4 -d sha256=FC6A87C7E9CB83DED48F099B6041B12AC9C6972776E73E23F98FC83F9D90148C https://androzoo.uni.lu/api/download"
                    curl = "sudo curl -O --remote-header-name -G -d apikey=" + API_KEY + " -d sha256=" + SHA256 + " \
                                    https://androzoo.uni.lu/api/download"

                    os.chdir(DOWNLOAD_PATH)

                    child = pexpect.spawn(curl)
                    child.logfile = fout
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
                print(e, file)
                return None

    def process(self):
        files = getFileList(CSV_FOLDER, ".csv")
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
