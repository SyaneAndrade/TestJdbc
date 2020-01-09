package br.com.lab.jdbc;

import java.io.IOException;
import java.sql.*;
import br.com.lab.jdbc.io.QueryIO;
import br.com.lab.jdbc.hive.sql.JdbcHiveRun;
import br.com.lab.jdbc.presto.sql.JdbcPrestoRun;

public class Main {

    public static void main(String[] args) throws SQLException, IOException {

        QueryIO qrio = new QueryIO(args[1]);


        JdbcHiveRun jdbcrun = new JdbcHiveRun(args[0]);

        String[] querys = qrio.lerQuery();

        for(String query: querys) {
            jdbcrun.executeQuery(query);
            }

        JdbcPrestoRun jdbcprestorun = new JdbcPrestoRun(args[2]);

        QueryIO qrio_presto = new QueryIO(args[3]);
        String[] querys2 = qrio_presto.lerQuery();

        for (String query: querys2){
            jdbcprestorun.executeQuery(query);
        }
    }
}
