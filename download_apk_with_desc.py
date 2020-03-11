# coding:utf-8
import os
import threadpool
import pexpect

API_KEY = "fa08a4ad8d8c9d3c56236d27bd9b99bb83c66c3fd65642d496ea2cbd13d4e8a4"

DOWNLOAD_PATH = "/mnt/fit-Knowledgezoo/yanjie/APK_with_DESC"
INPUT_DIR = "/home/yanjie/APIRecommendation/Description"
CHECK_PATH = "/mnt/fit-Knowledgezoo-vault/vault/apks/"
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
                # file = os.path.join(parent, filename)
                filePath.append(filename[:-len(pickstr)])
    return filePath


class Analysis:
    def __init__(self):
        self.max_jobs = 15

    def process_one(self, args):
        SHA256 = args
        try:
            if os.path.exists(os.path.join(DOWNLOAD_PATH, SHA256 + ".apk")):
                return
            if os.path.exists(os.path.join(CHECK_PATH, SHA256 + ".apk")):
                exi = os.path.join(CHECK_PATH, SHA256 + ".apk")
                now = os.path.join(DOWNLOAD_PATH, SHA256 + ".apk")
                CMD = "sudo cp " + exi + " " + now
                child = pexpect.spawn(CMD)
                child.logfile = fout
                index = child.expect(['password', pexpect.EOF, pexpect.TIMEOUT])
                if index == 0:
                    child.sendline(passwd)
                elif index == 1:
                    print("EOF!")
                else:
                    print("Time out!")
                child.close(force=True)
                return

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
            print(e, SHA256)
            return None

    def process(self):
        files = getFileList(INPUT_DIR, ".txt")
        args = [file for file in files]
        pool = threadpool.ThreadPool(self.max_jobs)
        requests = threadpool.makeRequests(self.process_one, args)
        [pool.putRequest(req) for req in requests]
        pool.wait()

    def start(self):
        self.process()


if __name__ == '__main__':
    analysis = Analysis()
    analysis.start()
    fout.close()
