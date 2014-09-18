## Summary

This set of scripts will help build the tables and conversion SQL to migrate from TextFiles to ORC formatted files in Hive.

These scripts do NOT use the Hive CLI, they tap directly into the Hive Metastore database on MySQL.  So you will need access to the MySql database in order to run these scripts.

The core scripts are written in Groovy and will require groovy in the PATH.  Although, a complementary set of shell scripts, sharing a common name, can be used to minimize the required commandline parameters.  To use these shell scripts, the [util-env.sh] file needs to be updated to include the location and credentials for the MySql database used to store the hive metastore.

The listed scripts write results to stdout, so use [io redirects|http://www.tldp.org/LDP/abs/html/io-redirection.html] to build files that will be run via 'hive -f ..'

Use "--help" to get a list of options for the associated script.

## Selecting a user to run the resulting scripts

Once the scripts below are used to create the artifacts for the conversion, these scripts should be run by the user that owns the table.

## Workflow

The scripts require a _hive database_ name, along with a comma separated list of _hive tables_ in the listed database.

1. [ctl.sh] or [ctl.groovy] is used to create an alternate table, based on the original source table with an "ORC" format.  This process will only pull tables that are "TEXTFILE" tables in hive.
2. Validate that the table(s) have been created.  The new table name(s) will be in the same database, with the same name with an added "_orc" extension.
3. [migrate.sh] or [migrate.groovy] is used to build the hive scripts that will translate data from the original "TEXTFILE" formatted hive table, to the "ORC" formatted replacement.  The "ORC" table from the script above should already be created, prior to running this script.
4. Validate that the new table(s) have been populated and that the partitions (if it's a partitioned table) exist in the metastore. 

## Partition Transfers

Step #3 above will construct the migration scripts with ranges, when the tables are partitioned.  The number of partitions included 'a' transfer statement can be controlled via the '-pp' option in [migrate.groovy].

With '-pp', only the first partition element is considered.  The calculated range is based on the number of unique 'first' partition elements.  For example: If the table has two partition elements like month and day, if -pp is 4, then the range will be across 4 months, regardless of the number of day partitions. 