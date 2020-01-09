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
//        String tableName = "t_sap_nf_condicao_csv";
//        stmt.executeQuery("select * from  " + tableName + " limit 1");
        ResultSet res = stmt.executeQuery("show databases");
//        res = stmt.executeQuery("show databases");
        //  tables
        //String sql = "show tables '" + tableName + "'";
        //System.out.println("Running: " + sql);
        //res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }
        // describe table
//        sql = "describe " + tableName;
//        System.out.println("Running: " + sql);
//        res = stmt.executeQuery(sql);
//        while (res.next()) {
//            System.out.println(res.getString(1) + "\t" + res.getString(2));
//        }
        // load data into table
        // NOTE: filepath has to be local to the hive server
        // NOTE: /tmp/test_hive_server.txt is a ctrl-A separated file with two fields per line
//        String filepath = "/tmp/test_hive_server.txt";
//        sql = "load data local inpath '" + filepath + "' into table " + tableName;
//        System.out.println("Running: " + sql);
//        res = stmt.executeQuery(sql);
//        // select * query
//        sql = "select * from " + tableName;
//        System.out.println("Running: " + sql);
//        res = stmt.executeQuery(sql);
//        while (res.next()){
//            System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
//        }
        // regular hive query
//        sql = "select count(1) from " + tableName;
//        System.out.println("Running: " + sql);
//        res = stmt.executeQuery(sql);
//        while (res.next()) {
//            System.out.println(res.getString(1));
//        }

    }
}