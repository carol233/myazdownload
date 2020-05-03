# coding:utf-8
import csv
import os
import pandas as pd

LATESTCSV = "/mnt/fit-Knowledgezoo/Patrick/latest/latest.csv"
MARKET = "play.google.com"
CSV_FOLDER = "csv_folder"
HEADERS = []
line_count = {}
LINE_COUNT_SAVE = "line_count.csv"


def classify(pkg_name, markets, row):
    if MARKET not in markets:
        return
    csv_name = os.path.join(CSV_FOLDER, pkg_name + ".csv")

    if pkg_name in line_count:
        line_count[pkg_name] += 1
    else:
        line_count[pkg_name] = 1

    if not os.path.exists(csv_name):
        with open(csv_name, "w", newline="") as fw:
            writer = csv.writer(fw)
            writer.writerow(HEADERS)

    with open(csv_name, "a+", newline="") as fw:
        writer = csv.writer(fw)
        writer.writerow(row)

    return


if __name__ == '__main__':
    if not os.path.exists(CSV_FOLDER):
        os.mkdir(CSV_FOLDER)

    df = pd.read_csv(LATESTCSV, header=0, chunksize=10000)
    for chunk in df:
        for i in range(len(chunk)):
            HEADERS = chunk.columns.values.tolist()
            k = chunk.iloc[i]['pkg_name']
            m = chunk.iloc[i]['markets']
            classify(k, m, chunk.iloc[i].tolist())

    map_e_to_num_sort_list = sorted(line_count.items(), key=lambda d: d[1], reverse=True)

    with open(LINE_COUNT_SAVE, "w", newline="") as fw:
        writer = csv.writer(fw)
        for e in map_e_to_num_sort_list:
            pkg_name = e[0]
            num = e[1]
            writer.writerow([pkg_name, num])