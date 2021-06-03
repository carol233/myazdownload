# coding:utf-8
import csv
from datetime import datetime
import os
import pandas as pd

CSV_FOLDER = "/data/sdc/yanjie/androzoo/lineages/csv_folder"
MONTH_FOLDER = "/data/sdc/yanjie/androzoo/lineages/month_folder"
LINE_COUNT_SAVE = "/data/sdc/yanjie/androzoo/lineages/month_line_count.csv"
month_line_dict = {}


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

            if i_year < 2015:
                continue

            if i_year in date_dict:
                if i_month in date_dict[i_year]:
                    pre_date = date_dict[i_year][i_month][0]
                    if i_day > pre_date.day and i_time > pre_date.time():
                        date_dict[i_year][i_month] = [date, line]
                else:
                    date_dict[i_year][i_month] = [date, line]
            else:
                date_dict[i_year] = {}
                date_dict[i_year][i_month] = [date, line]

        key_list = sorted(date_dict.keys())
        for year in key_list:
            year_dict = date_dict[year]
            month_list = sorted(year_dict.keys())
            for month in month_list:
                results.append(year_dict[month][1])

        return results

    except Exception:
        print(file)
        exit()


if __name__ == '__main__':
    if not os.path.exists(MONTH_FOLDER):
        os.mkdir(MONTH_FOLDER)

    files = getFileList(CSV_FOLDER, ".csv")
    for file in files:
        results = select_ver(file)
        if len(results) <= 2:
            continue

        month_line_dict[os.path.split(file)[-1][:-4]] = len(results) - 1
        SAVE = os.path.join(MONTH_FOLDER, os.path.split(file)[-1])
        with open(SAVE, "w", newline="") as fw:
            writer = csv.writer(fw)
            for line in results:
                writer.writerow(line)

    map_e_to_num_sort_list = sorted(month_line_dict.items(), key=lambda d: d[1], reverse=True)
    with open(LINE_COUNT_SAVE, "w", newline="") as fw:
        writer = csv.writer(fw)
        for e in map_e_to_num_sort_list:
            pkg_name = e[0]
            num = e[1]
            writer.writerow([pkg_name, num])
