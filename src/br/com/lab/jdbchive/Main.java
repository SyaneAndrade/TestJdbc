package br.com.lab.jdbchive;

import java.io.IOException;
import java.sql.*;
import br.com.lab.jdbchive.io.QueryIO;
import br.com.lab.jdbchive.sql.JdbcHiveRun;

public class Main {

    public static void main(String[] args) throws SQLException, IOException {
        QueryIO qrio = new QueryIO("/home/jessiane-andrade/Downloads/test_hive_show_db.sql");
        JdbcHiveRun jdbcrun = new JdbcHiveRun();
        String[] querys = qrio.lerQuery();

        for(String query: querys) {
            jdbcrun.executeQuery(query);

            }
    }
}
