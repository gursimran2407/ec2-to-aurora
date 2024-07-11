# ec2-to-aurora

# MySQL to Aurora Migration

Migrating a MySQL database from an AWS EC2 instance to AWS Aurora MySQL can be a challenging but rewarding process. This repository contains scripts and documentation to guide you through the migration journey, addressing the challenges encountered and the solutions implemented to achieve a successful migration.

## Table of Contents
- [Introduction](#introduction)
- [Architecture](#architecture)
- [Evaluation of Options](#evaluation-of-options)
- [Selecting the Right DB Instance](#selecting-the-right-db-instance)
- [Preparing for Data Migration](#preparing-for-data-migration)
  - [Backup of Existing Database](#backup-of-existing-database)
  - [Backup Script](#backup-script)
- [Migrate Data to Aurora MySQL](#migrate-data-to-aurora-mysql)
  - [Restore Script](#restore-script)
- [Troubleshooting and Fine-Tuning](#troubleshooting-and-fine-tuning)
- [Validation and Findings](#validation-and-findings)
- [Conclusion](#conclusion)
- [License](#license)

## Introduction
This project aims to provide a comprehensive guide and the necessary scripts to migrate a MySQL database from an AWS EC2 instance to AWS Aurora MySQL. The migration process includes evaluation of options, preparation for data migration, actual data migration, troubleshooting, and validation.

## Architecture
![Migration Architecture](images/ec2-to-aurora.svg)

## Evaluation of Options
Our team assessed two options: Database Migration Service (DMS) and Aurora MySQL. DMS provides real-time replication monitoring and data validation features, while Aurora MySQL offers several benefits, including multiple copies across Availability Zones and intelligent storage.

## Selecting the Right DB Instance
The migration process involved creating an AWS RDS cluster for Aurora MySQL. Key factors considered included the required read replicas in different Availability Zones and intelligent storage to optimize performance.

## Preparing for Data Migration
### Backup of Existing Database
To begin the data migration, a backup of the existing MySQL database on the EC2 instance was necessary. The `mysqldump` utility was used to create a backup while ensuring data consistency with the `--single-transaction` option. Additionally, `DEFINER` statements were removed from the backup using a `sed` command to prevent potential issues during the restore process.

### Backup Script
Below is the Python script used for database backup. The script reads the password from the `~/.my.cnf` file in the root user's home directory and performs the backup for specified databases and tables.

Please find backup_mysql.py under /src

## Migrate Data to Aurora MySQL
With the backup in hand, the data migration phase included connecting to the Aurora MySQL instance and restoring the backup.
Please find restore_to_aurora.py under /src

## Troubleshooting and Fine-Tuning
During the migration process, several challenges were encountered, including Lost connection to MySQL server during query and Cannot add foreign key constraint errors. Solutions included adjusting cluster parameters and exploring different backup options to ensure a smooth restoration process.

## Validation and Findings
After multiple restoration attempts and fine-tuning, data validation on the Aurora MySQL instance was performed. Despite some initial discrepancies, the migration was successfully completed with a stable and high-performance Aurora MySQL cluster.

## Conclusion
The migration journey from an AWS EC2 instance to AWS Aurora MySQL involved careful evaluation, troubleshooting, and fine-tuning. The key lessons learned include:

- Selecting the right database option based on specific needs and requirements.
- Conducting thorough backups and ensuring data consistency during the migration process.
- Monitoring and addressing potential errors and discrepancies during the restoration process.
- Fine-tuning cluster parameters to optimize performance.

## License
This project is licensed under the MIT License. See the LICENSE file for details.
