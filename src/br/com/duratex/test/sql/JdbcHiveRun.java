package br.com.duratex.test.sql;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import org.apache.hive.jdbc.HiveConnection;
public class JdbcHiveRun {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection("jdbc:hive2://ec2-54-236-25-225.compute-1.amazonaws.com:10000/");
        Statement stmt = con.createStatement();

        ResultSet res = stmt.executeQuery("show databases");

        while (res.next()) {
            System.out.println(res.getString(1));
        }
    }
}