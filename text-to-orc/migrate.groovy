#!/usr/local/bin/groovy

/**
 * Build the migration scripts that populate the tables built in ORC format, from ctl.groovy.
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

def PARALLEL_PARTITIONS = ""
if (options.pp) {
    PARALLEL_PARTITIONS = options.pp
} else {
    PARALLEL_PARTITIONS = 30
}
TYPE_POSTFIX = "_orc"

def database = options.hd

// Append "s" to the "t" to get all... i know, crazy, right...
options.hts.each { intable ->
    println "${intable}"

    // Prepare for dynamic queries.
    println "set hive.exec.dynamic.partition=true;\n" +
            "set hive.exec.dynamic.partition.mode=nonstrict;"

    sql.eachRow("select db.name, t.tbl_id, t.tbl_name, t.tbl_type, s.input_format, s.location from " +
            "dbs db inner join tbls t on db.db_id = t.db_id inner join sds s on t.sd_id = s.sd_id where s.input_format = 'org.apache.hadoop.mapred.TextInputFormat' and db.name='${database}' and t.tbl_name='${intable}'") { table ->

        def fields = []
        sql.eachRow("select c2.column_name, c2.type_name from " +
                "tbls t inner join sds s on t.sd_id = s.sd_id inner join cds c on s.cd_id = c.cd_id inner join columns_v2 c2 on c.cd_id = c2.cd_id " +
                "where t.tbl_id = $table.tbl_id order by c2.integer_idx") { column ->
            fields.add("$column.column_name")
        }

        def FIELDS = ""
        fields.each { field ->
            FIELDS = FIELDS + " " + field;
            if (field != fields.last()) {
                FIELDS = FIELDS + ","
            }
        }

        // Partitions
        // Definition
        def partition_def = []
        sql.eachRow("select p.pkey_name, p.pkey_type from tbls t inner join partition_keys p on t.tbl_id = p.tbl_id where t.tbl_id = $table.tbl_id order by p.integer_idx; ") { partition ->
            // TODO: For now, going to assume partitions are all of type STRING.
            partition_def.add("$partition.pkey_name")
        }
        def PARTITIONS = ""
        partition_def.each { partition ->
            PARTITIONS = PARTITIONS + " " + partition;
            if (partition != partition_def.last()) {
                PARTITIONS = PARTITIONS + ","
            }
        }

        def INSERT_STATEMENT = "INSERT OVERWRITE " + database + ".${intable}" + TYPE_POSTFIX + " "

        def where = []
        if (partition_def.size() > 0) {
            def partitions = []
            sql.eachRow("select p.part_name from tbls t inner join partitions p on " +
                    "t.tbl_id = p.tbl_id where t.tbl_id = '$table.tbl_id' order by p.part_name;") { partition ->
                partitions.add("$partition.part_name")
            }

            INSERT_STATEMENT = INSERT_STATEMENT + " PARTITION (" + PARTITIONS + ")\n"
            INSERT_STATEMENT = INSERT_STATEMENT + "   SELECT\n"
            INSERT_STATEMENT = INSERT_STATEMENT + "      " + FIELDS + "\n"
            INSERT_STATEMENT = INSERT_STATEMENT + "       ," + PARTITIONS + "\n"
            INSERT_STATEMENT = INSERT_STATEMENT + "   FROM " + intable + "\n"


            if (partitions.size() > 0) {
                // YES, we have partitions.
                int part_count = 0;
                // ? Handling List of Maps....
                // Output should be like:
                //   part >= '...' and part < '...'
                //   TODO: Last part should NOT be bound by 'end' range to ensure we include it in the conversion.
                def wherePart = []
                def lastPart
                partitions.each { actual_partition ->

                    def p_parts = actual_partition.split('/')
                    def p_part = p_parts[0] // only interested in the first partition element for filtering.
                    def kv = p_part.split('=')
                    def key = kv[0]
                    def value = kv[1]

                    if (actual_partition == partitions.last()) {
                        // Last partition, special Handling.
                        wherePart.add(key + " <= '" + value + "'")
                        println INSERT_STATEMENT + "   WHERE " + wherePart[0] + " and " + wherePart[1]
                    } else {
                        if (value == lastPart) {
                            // Nothing, continue to next partition.  This enables us to scan and build statements
                            // based on the "FIRST" partitions count.
                        } else {
                            lastPart = value

                            if (part_count == 0 || part_count == (PARALLEL_PARTITIONS - 1)) {
                                if (part_count == 0)
                                    wherePart.add(key + " >= '" + value + "'")
                                else if (part_count == PARALLEL_PARTITIONS - 1)
                                    wherePart.add(key + " <= '" + value + "'")

                            }
                            if (part_count >= PARALLEL_PARTITIONS - 1) {
                                // reset
                                // set where based on current where part.
                                println INSERT_STATEMENT + "   WHERE " + wherePart[0] + " and " + wherePart[1]
                                wherePart = []
                                part_count = 0;
                            } else {
                                part_count++;
                            }
                        }
                    }
                }
            } else {
                // No partitions found
            }
//            println where

        } else {
            // NO partitions defined.
        }

        // TODO: Build the swap commands.


    }
}
