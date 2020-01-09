package br.com.lab.jdbchive.sql;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class JdbcHiveRun {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static Connection con;
    public Statement stmt;

    public JdbcHiveRun() throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        con = DriverManager.getConnection("jdbc:hive2://ec2-54-236-25-225.compute-1.amazonaws.com:10000/");
        stmt = con.createStatement();
    }

    public  void executeQuery(String query) throws SQLException {

            ResultSet res = stmt.executeQuery(query);

            while (res.next()) {
                System.out.println(res.getString(1));
            }
    }
}