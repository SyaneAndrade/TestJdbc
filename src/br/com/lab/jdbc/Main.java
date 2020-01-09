package br.com.lab.jdbc;

import java.io.IOException;
import java.sql.*;
import br.com.lab.jdbc.io.QueryIO;
import br.com.lab.jdbc.hive.sql.JdbcHiveRun;
import br.com.lab.jdbc.presto.sql.JdbcPrestoRun;

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
