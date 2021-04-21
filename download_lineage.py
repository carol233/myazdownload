# coding:utf-8
import os
import pandas as pd
import sys
import threadpool
import pexpect

API_KEY = "fa08a4ad8d8c9d3c56236d27bd9b99bb83c66c3fd65642d496ea2cbd13d4e8a4"

DOWNLOAD_PATH = "/data/sdc/yanjie/APPLineage3w"
CSV_FOLDER = "/data/sdb/pei/year_folder"
# CHECK_PATH = "/mnt/fit-Knowledgezoo-vault/vault/apks/"
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
        line_count = self.row_count(file)
        print(file)
        with open(file, 'r') as f:
            try:
                df = pd.read_csv(f, header=0)
                all_lines_num = len(df)
                for i in range(all_lines_num):

                    if i != all_lines_num - 2:
                        continue

                    SHA256 = df.iloc[i]['sha256']
                    if os.path.exists(os.path.join(DOWNLOAD_PATH, SHA256 + ".apk")):
                        return

                    # if os.path.exists(os.path.join(CHECK_PATH, SHA256 + ".apk")):
                    #     exi = os.path.join(CHECK_PATH, SHA256 + ".apk")
                    #     now = os.path.join(DOWNLOAD_PATH, SHA256 + ".apk")
                    #     CMD = "sudo cp " + exi + " " + now
                    #     child = pexpect.spawn(CMD)
                    #     child.logfile = fout
                    #     index = child.expect(['password', pexpect.EOF, pexpect.TIMEOUT])
                    #     if index == 0:
                    #         child.sendline(passwd)
                    #     elif index == 1:
                    #         print("EOF!")
                    #     else:
                    #         print("Time out!")
                    #     child.close(force=True)
                    #     return

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

    def row_count(self, filename):
        with open(filename) as in_file:
            return sum(1 for _ in in_file)

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
