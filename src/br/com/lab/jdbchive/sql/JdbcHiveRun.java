package br.com.lab.jdbchive.sql;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class JdbcHiveRun {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static Connection con;
    public Statement stmt;

    public JdbcHiveRun(String caminhoHive) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        con = DriverManager.getConnection(caminhoHive);
        stmt = con.createStatement();
    }

    public  void executeQuery(String query) {

        ResultSet set = null;

        try {
            System.out.println(query);
            ResultSet res = stmt.executeQuery(query);
            while (res.next()) {
                System.out.println(res.getString(1));
            }
        }
        catch (SQLException e){
            e.printStackTrace();
        }
    }
}