import pymysql
import csv
import os
import argparse
import tqdm
import threading

search_dict = {
    "H": 1.0078250321,
    "Na": 22.98976967,
}


class MetaSearch:
    def __init__(self):
        pass

    def search_db(self, mz: float, ion: str = "H", tolerance: float = 0.01):
        search_mz = mz - search_dict[ion]
        location_str = f"WHERE monisotopic_molecular_weight BETWEEN {search_mz - tolerance} AND {search_mz + tolerance}"
        sql_str = f"SELECT * FROM hmdb {location_str}"
        self.cursor.execute(sql_str)
        return self.cursor.fetchall()

    def search_list(self, mz_list: list[float], save_path: str, ions: list[str], tolerance: float = 0.01):
        if not os.path.isdir(save_path):
            os.mkdir(save_path)
        print("Start searching...")
        for mz in tqdm.tqdm(mz_list):
            self.search_db_and_save(mz, ions, tolerance, save_path)
            
    def search_csv(self, csv_path: str, save_path: str, ions: list[str], tolerance: float = 0.01):
        with open(csv_path, "r", encoding="utf-8") as f:
            mz_list = [float(line.strip()) for line in f.readlines()]
        self.search_list(mz_list, save_path, ions, tolerance)

    def search_db_and_save(self, mz: float, ions: list[str], tolerance: float, save_path: str):
        result = []
        for ion in ions:
            result.extend(self.search_db(mz, ion, tolerance))
        with open(os.path.join(save_path, f"{mz}.csv"), "a", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["accession", "name", "chemical_formula", "monisotopic_molecular_weight", "description"])
            for i in result:
                writer.writerow(i)

    def __enter__(self):
        self.conn = pymysql.connect(
            host="192.168.200.90",
            user="scmeta",
            password="xrzhang2023",
            database="hmdb_local",
            port=3306
        )
        self.cursor = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()


if __name__ == '__main__':
    # argv: search_path, output_path, tolerance, ions
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", help="search file path")
    parser.add_argument("-o", "--output", help="output file path")
    parser.add_argument("-t", "--tolerance", help="tolerance", default=0.01)
    parser.add_argument("-i", "--ions", help="ions", nargs="+", default=["H", "Na"])
    args = parser.parse_args()
    print("Start reading file...")
    with MetaSearch() as ms:
        ms.search_csv(args.file, args.output, args.ions, args.tolerance)

