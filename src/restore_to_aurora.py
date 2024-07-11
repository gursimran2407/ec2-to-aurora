#!/usr/bin/python3 -u

import datetime
import json
import pprint
import time
import subprocess
from pathlib import Path
import os
import logging
import glob
import boto3

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

log = logging.getLogger("my-logger")

DB_PASS = os.environ.get('DB_PASSWORD')
databases = ["db1"]
cluster_endpoint = "asdss.region.rds.amazonaws.com"
db_user = "root"

def append_multiple_lines(file_name, lines_to_append):
    # Helper function to append multiple lines to a file
    with open(file_name, "a+") as file_object:
        appendEOL = False
        file_object.seek(0)
        data = file_object.read(100)
        if len(data) > 0:
            appendEOL = True
        for line in lines_to_append:
            if appendEOL == True:
                file_object.write("\n")
            else:
                appendEOL = True
            file_object.write(line)

class Restore:
    def __init__(self, mysql_cnf_file_path):
        self.mysql_cnf_file_path = mysql_cnf_file_path

    def dirs_files_log(self, database, table):
        # Helper function to create directories to store logs and counts
        filestamp = time.strftime('%d-%m-%Y_%H-%M-%S')

        db_restore_path = f"{database}/{table}"
        db_restore_logs_path = Path(f"{db_restore_path}/logs")
        db_restore_logs_path.mkdir(parents=True, exist_ok=True)
        std_out_path = db_restore_logs_path / f"{table}_{filestamp}_out.txt"
        std_err_path = db_restore_logs_path / f"{table}_{filestamp}_err.txt"
        table_count_path = db_restore_logs_path / f"{table}_{filestamp}_row_count.txt"
        std_out_path.touch(exist_ok=True)
        std_err_path.touch(exist_ok=True)
        # table_count_path.touch(exist_ok=True)

        return std_out_path, std_err_path, table_count_path, filestamp

    def get_sql_backups_from_s3_bucket(self, files_path_to_save, s3_bucket_name, object_uri):
        # Get files from an S3 bucket
        s3 = boto3.resource('s3')
        try:
            log.info(f"Downloading sql backup files from S3 bucket {s3_bucket_name}... URI: {object_uri}")
            s3_resource = boto3.resource('s3')
            bucket = s3_resource.Bucket(s3_bucket_name)
            for obj in bucket.objects.filter(Prefix=object_uri):
                if not os.path.exists(os.path.dirname(obj.key)):
                    os.makedirs(os.path.dirname(obj.key), exist_ok=True)
                bucket.download_file(obj.key, obj.key)  # save to the same path
        except ClientError as e:
            log.error(e)
            return False
        return True

    def restore_backups(self, database_name, sql_dump_files):
        # Restore backup from the input sql_dump_files list
        processes_restore = []
        time_dict = {}

        for file_name_to_import in sql_dump_files:
            # Getting table paths
            file_name_stripped_base_name = os.path.basename(file_name_to_import).strip()
            std_out_path, std_err_path, table_count_path, filestamp = self.dirs_files_log(database_name, file_name_stripped_base_name)
            std_out_path_str = str(std_out_path.resolve())
            std_err_path_str = str(std_err_path.resolve())

            restore_command = f"mysql --defaults-extra-file={self.mysql_cnf_file_path} -vv -h {cluster_endpoint} -D {database_name} --max-allowed-packet=256M < {file_name_to_import} 2>>{std_err_path_str} >>{std_out_path_str}"
            log.info(f"Restore Command \n for file: {file_name_stripped_base_name} \n\n Restore Command: {restore_command}")

            start_time = time.time()
            log.info(f"Restore start time for table: {file_name_stripped_base_name}: {start_time}")
            time_dict[file_name_stripped_base_name] = [start_time]
            write_json(time_dict, f"time_data_{cluster_endpoint}.json")

            # stdout=subprocess.PIPE,stderr=subprocess.PIPE,
            restore_p = subprocess.Popen(restore_command, shell=True)
            processes_restore.append((restore_p, file_name_stripped_base_name))
            log.info(f"\n\n {file_name_stripped_base_name} is being restored to {cluster_endpoint} ... \n")

        # Waiting for all the processes to complete and calculating time
        for p, file_name in processes_restore:
            p.wait()
            log.info(f"Waiting for process RESTORE for table : {file_name} \n")

            if p.returncode != 0:
                log.error(f"Restoring Database backup file: {file_name} to {cluster_endpoint} failed. Please check error logs...")

            process_time_list = time_dict[file_name]
            curr_time = time.time()
            process_time_list.append(curr_time)
            process_time_taken_to_restore = curr_time - process_time_list[0]
            process_time_list.append(process_time_taken_to_restore)

            time_str = f"Time taken to restore {file_name}: {process_time_taken_to_restore}"
            log.info(time_str)
            time_dict[file_name] = process_time_list

            # # Saving this to a file
            write_json(time_dict, f"time_data_{cluster_endpoint}.json")

            log.info(f"\n|| Restoring Database backup file: {file_name} to {cluster_endpoint} SUCCESS")

        pprint.pprint(time_dict)
        log.info(f"RESTORING Tables is FINISHED!! ... for {len(processes_restore)} tables")

if __name__ == "__main__":
    mysql_cnf_file_path = "/root/.aurora_rds.cnf"
    sql_dump_schema_path = "/mnt/nvme1n1/migration/backup/all_backups_prod/no_data_backup_files"
    sql_dump_data_path = "backup_files"

    big_split_sql_files_folder = "split_backup_files"

    r = Restore(mysql_cnf_file_path)

    bucket_name = "backups"
    s3_object_key_name = "rds_aurora_backup/backup_sql_files"

    # Getting the sql files from S3 Bucket
    r.get_sql_backups_from_s3_bucket(sql_dump_schema_path, bucket_name, s3_object_key_name)

    """ Now run the restore process which restores sql files present in the sql_dump_files var"""

    """ Restoring the table schema first """

    sql_dump_schema_files = glob.glob(f"{sql_dump_schema_path}/**/*.sql", recursive=True)
    log.info(f"Restoring sql Schema files - {sql_dump_schema_path} STARTED... \n Number of Files : {len(sql_dump_schema_files)}")
    r.restore_backups(databases[0], sql_dump_schema_files)
    log.info(f"Restoring sql Schema files FINISHED... - from {sql_dump_schema_path}")

    """ Restoring the table data now """
    sql_dump_data_files = glob.glob(f"{sql_dump_data_path}/**/*.sql", recursive=True)
    log.info(f"Restoring sql DATA (No Create Info) files - {sql_dump_data_path} STARTED... \n Number of Files : {len(sql_dump_data_files)}")
    r.restore_backups(databases[0], sql_dump_data_files)
    log.info(f"Restoring sql DATA (No Create Info) files FINISHED... {sql_dump_data_path}")

    log.info(f"Restore SCHEMA AND DATA SUCCESS!")  
