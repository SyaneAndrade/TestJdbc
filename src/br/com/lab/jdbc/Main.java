package br.com.lab.jdbc;

import java.io.IOException;
import java.sql.*;
import br.com.lab.jdbc.io.QueryIO;
import br.com.lab.jdbc.hive.sql.JdbcHiveRun;
import br.com.lab.jdbc.presto.sql.JdbcPrestoRun;

// jdbc:hive2://ec2-54-236-25-225.compute-1.amazonaws.com:10000/ none none /home/jessiane-andrade/Downloads/test_hive_show_db.sql jdbc:presto://ec2-54-236-25-225.compute-1.amazonaws.com:8889/hive?user=Hive?SSL=true Hive none

public class Main {

    public static void main(String[] args) throws SQLException, IOException {

        QueryIO qrio = new QueryIO(args[3]);

        String userHive = ((args[1] != "none") ? args[1] : "");
        String passHive = ((args[2] != "none") ? args[1] : "");
        String userPresto = ((args[5] != "none") ? args[1] : "");
        String passPresto = ((args[6] != "none") ? args[1] : "dummy_password");

        JdbcHiveRun jdbcrun = new JdbcHiveRun(args[0], userHive, passHive);

        String[] querys = qrio.lerQuery();

        for(String query: querys) {
            jdbcrun.executeQuery(query);
            }

        JdbcPrestoRun jdbcprestorun = new JdbcPrestoRun(args[4], userPresto, passPresto);

        for (String query: querys){
            jdbcprestorun.executeQuery(query);
        }
    }
}
