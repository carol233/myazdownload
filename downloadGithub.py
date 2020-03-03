import os
import pandas as pd
import threadpool
import git  # pip3 install gitpython

DOWNLOAD_PATH = "/home/yanjie/azdownload/Github_Code"
CSV_FOLDER = "/home/yanjie/azdownload/AndroZooGithub"
GITHUB_CSV = "/home/yanjie/azdownload/csv_file_final.csv"
script_filename = "Github_log"
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
        self.map = {}

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
                    if os.path.exists(os.path.join(DOWNLOAD_PATH, SHA256)):
                        continue
                    pkg_name = df.iloc[i]['pkg_name']

                    # https://github.com/ohmyzsh/ohmyzsh.git
                    # /00-00-00/ably-chat

                    git_path = 'git@github.com:' + self.map[pkg_name] + '.git'
                    single_path = os.path.join(DOWNLOAD_PATH, SHA256)
                    new_repo = git.Repo.clone_from(url=git_path, to_path=single_path)
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
        with open(GITHUB_CSV, "r") as fr:
            try:
                df = pd.read_csv(fr, header=None)
                for i in range(len(df)):
                    pkg_name = df.iloc[i, 10]
                    github_name = df.iloc[i, 0]
                    print(pkg_name, github_name)
                    if github_name == "" or pkg_name == "":
                        continue
                    self.map[pkg_name] = github_name
            except Exception as e:
                print(e, GITHUB_CSV)
                return None
        self.process()

if __name__ == '__main__':
    if not os.path.exists(DOWNLOAD_PATH):
        os.mkdir(DOWNLOAD_PATH)
    analysis = Analysis()
    analysis.start()
    fout.close()
