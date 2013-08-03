/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author volker
 */
public class DatabaseTestScenario extends TstBaseClass {
    
    protected static final String DB_USER = "unittest";
    protected static final String DB_PASSWD = "unittest";
    protected static final String MYSQL_HOST = "localhost";
    protected static final String MYSQL_DB = "unittest";
    protected static final String MYSQL_PORT = "3306";
    
    
    protected Connection getMysqlConn(boolean openDB) throws SQLException
    {
        String connStr = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/";
        
        if (openDB) connStr += MYSQL_DB;
        
        return DriverManager.getConnection(connStr, DB_USER, DB_PASSWD);
    }
    
    protected void cleanupMysql() throws SQLException
    {
        Connection c = getMysqlConn(false);
        Statement s = c.createStatement();
        s.execute("DROP DATABASE " + MYSQL_DB);
        s.execute("CREATE DATABASE " + MYSQL_DB);
        s.close();
        c.close();
    }
    
    protected void prepMysqlScenario01() throws SQLException
    {
        cleanupMysql();
        Connection c = getMysqlConn(true);
        Statement s = c.createStatement();
        
        s.execute("CREATE TABLE IF NOT EXISTS t1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT," +
                " i INT, f DOUBLE, s VARCHAR(40), d DATETIME)");
        
        s.execute("INSERT INTO t1 VALUES (NULL, 42, 23.23, 'Hallo', NOW())");
        s.execute("INSERT INTO t1 VALUES (NULL, NULL, 666.66, 'Hi', NOW())");
        s.execute("INSERT INTO t1 VALUES (NULL, 84, NULL, 'Ho', NOW())");
        s.execute("INSERT INTO t1 VALUES (NULL, 84, NULL, 'Ho', NOW())");
        
        s.execute("CREATE OR REPLACE VIEW v1 AS SELECT * FROM t1 WHERE i=84");
    }
}
