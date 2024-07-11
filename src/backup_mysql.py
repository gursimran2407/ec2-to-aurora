#!/usr/bin/python3
"""
Will backup all the databases listed, will put files in the same directory as the script.
To run: $ python3 b.py
This script reads the password from the ~/.my.cnf file in the root user's home directory.
"""

import time
import subprocess
from pathlib import Path
import os
import logging
import glob
import boto3

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
log = logging.getLogger("my-logger")

DB_USER = 'root'
databases = ["db1"]
tables = [
    # List of table names...
]

def dirs_files_log(database, table):
    # Creating directories to store logs and counts
    filestamp = time.strftime('%d-%m-%Y_%H-%M-%S')

    db_backup_path = f"{database}/{table}"
    db_backup_logs_path = Path(f"{db_backup_path}/logs")
    db_backup_logs_path.mkdir(parents=True, exist_ok=True)
    std_out_path = db_backup_logs_path / f"{table}_{filestamp}_out.txt"
    std_err_path = db_backup_logs_path / f"{table}_{filestamp}_err.txt"
    table_count_path = db_backup_logs_path / f"{table}_{filestamp}_row_count.txt"
    std_out_path.touch(exist_ok=True)
    std_err_path.touch(exist_ok=True)
    table_count_path.touch(exist_ok=True)

    return std_out_path, std_err_path, table_count_path, filestamp

def get_dump(database, table):
    processes = []
    for database in databases:
        for table in tables:
            std_out_path, std_err_path, table_count_path, filestamp = dirs_files_log(database, table)
            file_name = f"{table}_{filestamp}".strip()

            # Before getting the dump - taking row count for the table
            run_sql_command(database, table_count_path, f"SELECT COUNT(*) FROM {table};")

            dump_args = [
                "mysqldump", "--defaults-extra-file=/root/.my.prod.cnf", "--verbose",
                "--master-data=2", "--single-transaction", "--set-gtid-purged=OFF",
                "--skip-extended-insert", "--order-by-primary", "--max-allowed-packet=256M",
                f"-r{file_name}.sql", f"-u{DB_USER}", f"{database}", f"{table}"
            ]

            log.info(dump_args)
            log.info(f"Creating a dump for {table}...")
            # Creating a dump file and saving logs to the appropriate folder.
            with open(std_out_path, "wb") as out, open(std_err_path, "wb") as err:
                p = subprocess.Popen(dump_args, stdout=out, stderr=err)
                processes.append((p, table))
                log.info(f"\n|| Database is being dumped to {file_name}.sql ")

    # Waiting for all the processes to complete
    for p, table_name in processes:
        p.wait()
        log.info(f"Waiting for process backup for table: {table_name} \n")
        if p.returncode != 0:
            print("Creating Dump failed:")
            print("Output was:", p.stdout)
            raise Exception(f"Failed to take database backup: {table_name}")
        log.info(f"\n|| Database dumped to {table_name}.sql ")
    
    log.info(f"DUMPING is FINISHED... for {len(processes)} tables")

def run_sql_command(database, output_file_path, query):
    mysql_connect_args = [
        "mysql", "--defaults-extra-file=/root/.my.prod.cnf", "--verbose",
        f"-u{DB_USER}", f"{database}", f"-e {query}"
    ]

    with open(output_file_path, "wb") as file:
        log.info("Connecting to mysql...")
        process = subprocess.Popen(mysql_connect_args, stdout=file, stderr=subprocess.STDOUT, stdin=subprocess.PIPE)
        # stdout, _ = process.communicate(input=query)
        process.wait()
        log.info(f"Output: {process.stdout}")
        if process.returncode != 0:
            print("Query failed:", query)
            raise Exception(f"Failed to lookup query: {query}")

def upload_files(bucket, file_path, object_name):
    """Upload a file to an S3 bucket
    :param bucket: Bucket to upload to
    :param file_path: Path to the directory containing sql files
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if files were uploaded, else False
    """
    sql_dump_files = glob.glob(f"{file_path}/*.sql")
    # Upload the files
    s3 = boto3.resource('s3')
    try:
        for sql_file in sql_dump_files:
            sql_file_base_name = os.path.basename(sql_file).strip()
            print(sql_file_base_name)
            s3_final_obj_name = f"{object_name}/{sql_file_base_name}"
            log.info(f"Uploading {sql_file} to s3 bucket {bucket}...{s3_final_obj_name}")
            response = s3.meta.client.upload_file(sql_file, bucket, s3_final_obj_name)
    except boto3.exceptions.S3UploadFailedError as e:
        log.error(e)
        return False
    return True

if __name__ == "__main__":
    # Dump each table into a sql file.
    log.info(f"Starting backup for tables: {tables} \n\n")
    log.info(f"Number of tables: {len(tables)} \n\n")
    get_dump(databases, tables)

    # Upload all dump files to an s3 Bucket
    sql_path = "/mnt/nvme0n1/backup_test"
    bucket_name = "-backups"
    s3_object_key_name = "rds_aurora_backup/backup_sql_files"
    upload_files(bucket_name, sql_path, s3_object_key_name)
