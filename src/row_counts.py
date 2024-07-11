#!/usr/bin/python3 -u

import json
import pprint
import time
import subprocess
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("my-logger")

HOST = 'localhost'
PORT = '3306'
DB_USER = 'root'
databases = ["db1"]
table_names_file_path = "table_names.txt" # table names in each line
cluster_endpoint = "324234.ca-central-1.rds.amazonaws.com"  # Aurora cluster endpoint
tables = []
with open(table_names_file_path) as f:
    for line in f:
        line = line.strip()  # preprocess line
        tables.append(line)


def write_json(new_data, filename='data.json'):
    with open(filename, 'w+') as file:
        # First we load existing data into a dict.
        file.seek(0)  # rewind
        json.dump(new_data, file, indent=4)
        file.truncate()


def create_row_counts_json(user="root", hostname="localhost", default_extra_file="/root/.my.prod.cnf"):
    row_counts_tables = {}
    timestr = time.strftime("%Y%m%d-%H%M%S")
    for table_name in tables:
        try:
            query = f"SELECT COUNT(*) FROM {table_name}"
            row_cmd = f"mysql  --defaults-extra-file={default_extra_file} -h {hostname} -D {databases[0]} -e '{query}' -u {user} -sN"

            log.info(
                f"Connecting to mysql... {query} \n {table_name} \n args: {row_cmd}")
            out_str = subprocess.check_output(row_cmd, shell=True)

            log.info(
                f"Process started for {table_name} | row count | {hostname}")
            row_counts_tables[table_name] = out_str.strip().decode("utf-8")
            log.info(f"Writing row_count: {out_str} for table: {table_name} to json...")
            write_json(row_counts_tables,
                       f"row_counts_{hostname}_{timestr}.json")
        except subprocess.CalledProcessError as e:
            log.error(e)
            continue
    pprint.pprint(row_counts_tables)
    log.info(f"ROW_COUNTS calculated for {hostname} - SUCCESS")


if __name__ == "__main__":
    # Dump each table into a sql file.
    log.info(f"Starting row_counts for tables: {tables} \n\n")
    log.info(f"Number of tables: {len(tables)} \n\n")
    create_row_counts_json("root", cluster_endpoint, "/root/.aurora_rds.cnf")
    log.info("FINISHED!")
