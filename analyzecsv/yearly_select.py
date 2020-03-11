# coding:utf-8
import csv
from datetime import datetime
import os

import pandas as pd

CSV_FOLDER = "csv_folder"
YEAR_FOLDER = "year_folder"
LINE_COUNT_SAVE = "year_line_count.csv"
year_line_dict = {}


def getFileList(rootDir, pickstr):
    """
    :param rootDir:  root directory of dataset
    :return: A filepath list of sample
    """
    filePath = []
    for parent, dirnames, filenames in os.walk(rootDir):
        for filename in filenames:
            if pickstr in filename:
                file = os.path.join(parent, filename)
                filePath.append(file)
    return filePath


def select_ver(file):
    try:
        results = []
        df = pd.read_csv(file, header=0)
        HEADERS = df.columns.values.tolist()
        results.append(HEADERS)
        date_dict = {}
        for i in range(len(df)):
            dex_date = df.iloc[i]['dex_date']
            line = df.iloc[i].tolist()

            if "-" and ":" not in str(dex_date):
                continue
            date = datetime.strptime(str(dex_date), '%Y-%m-%d %H:%M:%S')
            i_year = date.year
            i_month = date.month
            i_day = date.day
            i_time = date.time()

            if i_year < 2013:
                continue

            if i_year in date_dict:
                pre_date = date_dict[i_year][0]
                if i_month > pre_date.month and i_day > pre_date.day and i_time > pre_date.time():
                    date_dict[i_year] = [date, line]
            else:
                date_dict[i_year] = [date, line]

        key_list = sorted(date_dict.keys())
        for item in key_list:
            results.append(date_dict[item][1])

        return results

    except Exception:
        print(file)
        exit()


if __name__ == '__main__':
    if not os.path.exists(YEAR_FOLDER):
        os.mkdir(YEAR_FOLDER)

    files = getFileList(CSV_FOLDER, ".csv")
    for file in files:
        results = select_ver(file)
        if len(results) <= 2:
            continue
        year_line_dict[os.path.split(file)[-1][:-4]] = len(results) - 1
        SAVE = os.path.join(YEAR_FOLDER, os.path.split(file)[-1])
        with open(SAVE, "w", newline="") as fw:
            writer = csv.writer(fw)
            for line in results:
                writer.writerow(line)

    map_e_to_num_sort_list = sorted(year_line_dict.items(), key=lambda d: d[1], reverse=True)
    with open(LINE_COUNT_SAVE, "w", newline="") as fw:
        writer = csv.writer(fw)
        for e in map_e_to_num_sort_list:
            pkg_name = e[0]
            num = e[1]
            writer.writerow([pkg_name, num])
