## Summary

This set of scripts will help build the tables and conversion SQL to migrate from TextFiles to ORC formatted files in Hive.

These scripts do NOT use the Hive CLI, they tap directly into the Hive Metastore database on MySQL.  So you will need access to the MySql database in order to run these scripts.

The script is written in Groovy and will require groovy in the PATH.  Although, a complementary set of shell scripts, sharing a common name, can be used to minimize the required commandline parameters.  To use these shell scripts, the [util-env.sh] file needs to be updated to include the location and credentials for the MySql database used to store the hive metastore.

Use "--help" to get a list of options for the associated script.

## Selecting a user to run the resulting scripts

Once the script below is used to create the artifacts for the conversion, the generated script should be run by the user that owns the table. Since the user will be the one that runs the Hive CLI which processes the generate DDL and DML.

## Workflow

The scripts require a _hive database_ name, along with a comma separated list of _hive tables_ in the listed database.

[convert.sh] or [convert.groovy] is used to create alternate table definition(s), based on the original source table with an "ORC" format.  This process will only pull tables that are "TEXTFILE" tables in hive. A required element of the script is an _output_ directory.  In this directory, the script will build a single DDL script that will be used to create all of the alternate tables.  A sub-directory for each table listed in the commandline will created and in it will be 1 of more scripts that will manage the movement of data between the original table and the new table.

A control script named *control.sh* will be created the will call the generated scripts in the proper order.  There is an option to have this script create "nohup" jobs.  For large tables or several tables, this is recommended to increase the parallelism of the process.  Otherwise, the script will be run in a sequential manner and may never utilize the full capacity of your cluster.

On the other hand, be careful how many tables (and know their sizes) you use per run, as this is also a way to manage the amount of work you push out to the cluster.

## Partition Transfers

For tables with partitions, the generated scripts with be build out by ranges.  The number of partitions included 'in' a transfer statement can be controlled via the '-pp' option in [convert.groovy].

With '-pp', only the first partition element is considered.  The calculated range is based on the number of unique 'first' partition elements.  For example: If the table has two partition elements like month and day, if -pp is 4, then the range will be across 4 months, regardless of the number of day partitions. 
