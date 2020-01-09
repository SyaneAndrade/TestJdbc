package br.com.duratex.sql;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import br.com.duratex.io.QueryIO;

public class JdbcHiveRun {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException, IOException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }

        QueryIO qrio = new QueryIO("/home/jessiane-andrade/Downloads/test_hive_show_db.sql");
        String[] querys = qrio.lerQuery();
        Connection con = DriverManager.getConnection("jdbc:hive2://ec2-54-236-25-225.compute-1.amazonaws.com:10000/");

        Statement stmt = con.createStatement();
        for(String query: querys) {
            ResultSet res = stmt.executeQuery(query);

            while (res.next()) {
                System.out.println(res.getString(1));
            }
        }
    }
}