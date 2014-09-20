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
cli.nohup(longOpt: 'nohup', args: 1, required: false, 'Build scripts to run each load sql in its nohup thread')
cli.output(longOpt: 'output-dir', args: 1, required: true, 'Output Directory for scripts')
cli.mh(longOpt: 'metastore-host', args: 1, required: true, 'Metastore Database Host')
cli.mu(longOpt: 'metastore-user', args: 1, required: true, 'Metastore Database Username')
cli.mp(longOpt: 'metastore-password', args: 1, required: true, 'Metastore Database Password')
cli.md(longOpt: 'metastore-database', args: 1, required: true, 'Metastore Database')
cli.ahb(longOpt: 'alt-hdfs-base', args: 1, required: false, 'Alternate HDFS Tables Base Directory')

def options = cli.parse(this.args)

sql = Sql.newInstance('jdbc:mysql://' + options.mh + ':3306/' + options.md, options.mu, options.mp, 'com.mysql.jdbc.Driver')

TYPE_POSTFIX = "_orc"

def PARALLEL_PARTITIONS = 30
if (options.pp) {
    PARALLEL_PARTITIONS = options.pp.toInteger()
} else {
    PARALLEL_PARTITIONS = 30
}
TYPE_POSTFIX = "_orc"

def database = options.hd

def controlfile

// Cleanup and/or prep for new output.

def outputdir = new File(options.output);
// Refresh
if (outputdir.exists()) {
    println "Deleting existing Directory"
    outputdir.delete()
}
outputdir.mkdir();

// Create DDL File

// DDL Cmd Array
def ddl_statements = []
def rename_statements = []
def ddl_cleanup = []
def external_tbl_cleanup = []

HIVE_SET="set hive.exec.dynamic.partition=true;\n" +
"set hive.exec.dynamic.partition.mode=nonstrict;"
def controlcmds = []

// Append "s" to the "t" to get all... i know, crazy, right...
options.hts.each { intable ->
    //println "${intable}"

    // Build DDL
    sql.eachRow("select db.name, t.tbl_id, t.tbl_name, t.tbl_type, s.input_format, s.location from " +
            "DBS db inner join TBLS t on db.db_id = t.db_id inner join SDS s on t.sd_id = s.sd_id where s.input_format = 'org.apache.hadoop.mapred.TextInputFormat' and db.name='${database}' and t.tbl_name='${intable}'") { table ->
//    println "$table.name, $table.tbl_name, $table.tbl_type, $table.input_format, $table.location"
//        println "USE $database;"
        def CREATE_STATEMENT
        if ("$table.tbl_type" == "EXTERNAL_TABLE") {
            CREATE_STATEMENT = "CREATE EXTERNAL TABLE IF NOT EXISTS $table.tbl_name" + TYPE_POSTFIX + " (\n"
        } else {
            CREATE_STATEMENT "CREATE TABLE IF NOT EXISTS $table.tbl_name" + TYPE_POSTFIX + " (\n"
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
        CREATE_STATEMENT = CREATE_STATEMENT + COLUMNS + ")\n"

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
            CREATE_STATEMENT = CREATE_STATEMENT + "PARTITIONED BY (\n" + PARTITIONS + "\n)\n"
        }

        // LOCATION if EXTERNAL
        if ("$table.tbl_type" == "EXTERNAL_TABLE") {
            // STORED AS
            CREATE_STATEMENT = CREATE_STATEMENT + "STORED AS ORC\n"
            external_tbl_cleanup.add("hdfs dfs -rm -r $table.location")
            if (!options.ahb)
                location = "$table.location" + "_orc"
            else
                location = options.ahb + "/" + intable + "_orc"
            CREATE_STATEMENT = CREATE_STATEMENT +  "LOCATION '" + location + "';"
        } else {
            // STORED AS
            CREATE_STATEMENT = CREATE_STATEMENT + "STORED AS ORC;"
        }

        rename_statements.add("ALTER TABLE $table.tbl_name RENAME TO $table.tbl_name" + "_org;")
        rename_statements.add("ALTER TABLE $table.tbl_name" + TYPE_POSTFIX + " RENAME TO $table.tbl_name;")

        ddl_cleanup.add("DROP TABLE " + table.tbl_name + "_org;")

        ddl_statements.add(CREATE_STATEMENT)
    }


//    if (options.nohup.asBoolean() != true) {
//        // Prepare for dynamic queries.
//        println HIVE_SET
//    }

    sql.eachRow("select db.name, t.tbl_id, t.tbl_name, t.tbl_type, s.input_format, s.location from " +
            "DBS db inner join TBLS t on db.db_id = t.db_id inner join SDS s on t.sd_id = s.sd_id where s.input_format = 'org.apache.hadoop.mapred.TextInputFormat' and db.name='${database}' and t.tbl_name='${intable}'") { table ->


//        if (options.nohup.asBoolean() != true)
//            println "USE $database;"

        def fields = []
        sql.eachRow("select c2.column_name, c2.type_name from " +
                "TBLS t inner join SDS s on t.sd_id = s.sd_id inner join CDS c on s.cd_id = c.cd_id inner join COLUMNS_V2 c2 on c.cd_id = c2.cd_id " +
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
        sql.eachRow("select p.pkey_name, p.pkey_type from TBLS t inner join PARTITION_KEYS p on t.tbl_id = p.tbl_id where t.tbl_id = $table.tbl_id order by p.integer_idx; ") { partition ->
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

        def INSERT_STATEMENT = "INSERT OVERWRITE TABLE $intable" + TYPE_POSTFIX + " "

        def where = []
        if (partition_def.size() > 0) {
            def partitions = []
            sql.eachRow("select p.part_name from TBLS t inner join PARTITIONS p on " +
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
                int part_file_count = 0;
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
                        INSERT_STATEMENT_WITH_WHERE = INSERT_STATEMENT + "\n   WHERE " + wherePart[0] + " and " + wherePart[1] + ";"
                        part_file_count++;
                        // Create output file for partition set.
                        def partfile_name = intable+"/part_"+part_file_count+".sql"
                        if (! new File(options.output+"/"+intable).exists())
                            new File(options.output+"/"+intable).mkdir()
                        new File(options.output+"/"+partfile_name).withWriter { partfile ->
                            // Add use..
                            partfile.writeLine("USE $database;")
                            // Add Set Commands
                            partfile.writeLine(HIVE_SET)
                            // Add Insert..
                            partfile.writeLine(INSERT_STATEMENT_WITH_WHERE)
                            // Add to control file.
                            controlcmds.add("hive -f $partfile_name")
//                                controlfile.withWriter { cout ->
//                                    cout.writeLine("nohup hive -f $partfile_name &")
//                                }
                        }
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
                                INSERT_STATEMENT_WITH_WHERE = INSERT_STATEMENT + "\n   WHERE " + wherePart[0] + " and " + wherePart[1] + ";"
                                part_file_count++;
                                // Create output file for partition set.
                                def partfile_name = intable+"/part_"+part_file_count+".sql"
                                if (! new File(options.output+"/"+intable).exists())
                                    new File(options.output+"/"+intable).mkdir()
                                new File(options.output+"/"+partfile_name).withWriter { partfile ->
                                    // Add use..
                                    partfile.writeLine("USE $database;")
                                    // Add Set Commands
                                    partfile.writeLine(HIVE_SET)
                                    // Add Insert..
                                    partfile.writeLine(INSERT_STATEMENT_WITH_WHERE)
                                    // Add to control file.
                                    controlcmds.add("hive -f $partfile_name")

                                }
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


        } else {
            // NO partitions defined.
        }


        // TODO: Build the swap commands.


    }
}

ddl_file = new File(options.output+"/build_ddl.sql")
ddl_file.withWriter { ddlout ->
    ddlout.writeLine("use $database;")
    ddl_statements.each { ddl ->
        ddlout.writeLine(ddl)
    }
}

rename = new File(options.output + "/rename.sql")
rename.withWriter { ren ->
    ren.writeLine("use $database;")
    rename_statements.each { ren_st ->
        ren.writeLine(ren_st)
    }

}

ddl_cleanup

dclean = new File(options.output + "/ddl_cleanup.sql")
dclean.withWriter { dc ->
    dc.writeLine("use $database;")
    ddl_cleanup.each { cln_st ->
        dc.writeLine(cln_st)
    }

}

extClean = new File(options.output + "/ext_cleanup.sh")
extClean.withWriter { dc ->
    external_tbl_cleanup.each { cln_st ->
        dc.writeLine(cln_st)
    }

}

controlfile = new File(options.output+"/control.sh")
controlfile.withWriter { cout ->
    cout.writeLine("#!/bin/bash")
    cout.writeLine("cd `dirname \$0`")
    cout.writeLine("hive -f build_ddl.sql")
    controlcmds.each { cmd ->
        if (options.nohup.asBoolean() == true)
            cout.writeLine("nohup $cmd > "+options.output+".nohup.out &")
        else
            cout.writeLine(cmd)
    }
}
