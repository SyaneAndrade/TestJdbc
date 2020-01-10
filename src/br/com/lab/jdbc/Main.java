package br.com.lab.jdbc;

import java.io.IOException;
import java.sql.*;
import br.com.lab.jdbc.io.QueryIO;
import br.com.lab.jdbc.hive.sql.JdbcHiveRun;
import br.com.lab.jdbc.presto.sql.JdbcPrestoRun;

public class Main {

    public static void main(String[] args) throws SQLException, IOException {

        QueryIO qrio = new QueryIO(args[3]);

        String urlHiveJdbc = "";
        String urlPrestoJdbc = "";

        String userHive = ((args[1] != "none")) ? args[1] : "";
        String passwordHive = ((args[2] != "none")) ? args[2] : "";
        try {
             urlHiveJdbc = args[0];
        }
        catch(Exception e){
            System.out.println("Necessário para como primeiro parametro a url para conexão jdbc com o Hive;");
        }

        try {
            urlPrestoJdbc = args[4];
        }
        catch(Exception e){
            System.out.println("Necessário para como primeiro parametro a url para conexão jdbc com o Presto;");
        }

        JdbcHiveRun jdbcrun = new JdbcHiveRun(urlHiveJdbc, userHive, passwordHive);

        String[] querys = qrio.lerQuery();

        for(String query: querys) {
            jdbcrun.executeQuery(query);
            }

        JdbcPrestoRun jdbcprestorun = new JdbcPrestoRun(urlPrestoJdbc);

        QueryIO qrio_presto = new QueryIO(args[5]);
        String[] querys2 = qrio_presto.lerQuery();

        for (String query: querys2){
            jdbcprestorun.executeQuery(query);
        }
    }
}
