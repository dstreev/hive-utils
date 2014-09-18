#!/usr/local/bin/groovy

/**
 * Create Table ... Like ... STORED AS ORC...
 *
 * While Hive supports CREATE TABLE LIKE, it only allows for changing the tables base location and doesn't provide
 * a means to "change" the tables format.
 *
 * Created by dstreev on 9/9/14.
 */

import groovy.sql.Sql
import groovyjarjarcommonscli.Option

def cli = new CliBuilder()
cli.hd(longOpt: 'hive-database', args: 1, required: true, 'Database to include, REQUIRED')
cli.ht(longOpt: 'hive-tables', args: Option.UNLIMITED_VALUES, valueSeparator: ',', required: true, 'Comma separated list of tables, REQUIRED')
cli.pp(longOpt: 'parallel-partitions', args: 1, required: false, 'Parallel Partition Count to process in statement(default 30)')
cli.mh(longOpt: 'metastore-host', args: 1, required: true, 'Metastore Database Host')
cli.mu(longOpt: 'metastore-user', args: 1, required: true, 'Metastore Database Username')
cli.mp(longOpt: 'metastore-password', args: 1, required: true, 'Metastore Database Password')
cli.md(longOpt: 'metastore-database', args: 1, required: true, 'Metastore Database')

def options = cli.parse(this.args)

sql = Sql.newInstance('jdbc:mysql://' + options.mh + ':3306/' + options.md, options.mu, options.mp, 'com.mysql.jdbc.Driver')

TYPE_POSTFIX = "_orc"

def database = options.hd

// Append "s" to the "t" to get all... i know, crazy, right...
options.hts.each { intable ->
    //println "${intable}"


    sql.eachRow("select db.name, t.tbl_id, t.tbl_name, t.tbl_type, s.input_format, s.location from " +
            "DBS db inner join TBLS t on db.db_id = t.db_id inner join SDS s on t.sd_id = s.sd_id where s.input_format = 'org.apache.hadoop.mapred.TextInputFormat' and db.name='${database}' and t.tbl_name='${intable}'") { table ->
//    println "$table.name, $table.tbl_name, $table.tbl_type, $table.input_format, $table.location"
        println "USE $database;"
        if ("$table.tbl_type" == "EXTERNAL_TABLE") {
            println "CREATE EXTERNAL TABLE $table.tbl_name" + TYPE_POSTFIX + " ("
        } else {
            println "CREATE TABLE $table.name" + "." + "$table.tbl_name" + TYPE_POSTFIX + " ("
        }
        def columns = []
        sql.eachRow("select c2.column_name, c2.type_name from " +
                "TBLS t inner join SDS s on t.sd_id = s.sd_id inner join CDS c on s.cd_id = c.cd_id inner join COLUMNS_V2 c2 on c.cd_id = c2.cd_id " +
                "where t.tbl_id = $table.tbl_id order by c2.integer_idx") { column ->
            columns.add("$column.column_name $column.type_name")
        }
        COLUMNS = ""
        columns.each { column ->
            COLUMNS = COLUMNS + "   " + column;
            if (column != columns.last()) {
                COLUMNS = COLUMNS + ",\n"
            }
        }
        println COLUMNS

        println ")"
        // Partitions
        def partitions = []
        PARTITIONS = ""
        sql.eachRow("select p.pkey_name, p.pkey_type from TBLS t inner join PARTITION_KEYS p on t.tbl_id = p.tbl_id where t.tbl_id = $table.tbl_id order by p.integer_idx; ") { partition ->
            partitions.add("$partition.pkey_name $partition.pkey_type")
        }
        partitions.each { partition ->
            PARTITIONS = PARTITIONS + "   " + partition;
            if (partition != partitions.last()) {
                PARTITIONS = PARTITIONS + ",\n"
            }
        }
        if (PARTITIONS.length() > 0) {
            println "PARTITIONED BY (\n" + PARTITIONS + "\n)"
        }

        // LOCATION if EXTERNAL
        if ("$table.tbl_type" == "EXTERNAL_TABLE") {
            // STORED AS
            println "STORED AS ORC"
            location = "$table.location" + "_orc"
            println "LOCATION '" + location + "';"
        } else {
            // STORED AS
            println "STORED AS ORC;"
        }

        println ""
    }

}
