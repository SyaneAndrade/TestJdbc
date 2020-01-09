package br.com.lab.jdbchive;

import java.io.IOException;
import java.sql.*;
import br.com.lab.jdbchive.io.QueryIO;
import br.com.lab.jdbchive.sql.JdbcHiveRun;

public class Main {

    public static void main(String[] args) throws SQLException, IOException {

        QueryIO qrio = new QueryIO(args[1]);
        JdbcHiveRun jdbcrun = new JdbcHiveRun(args[0]);
        String[] querys = qrio.lerQuery();

        for(String query: querys) {
            jdbcrun.executeQuery(query);
            }
    }
}
