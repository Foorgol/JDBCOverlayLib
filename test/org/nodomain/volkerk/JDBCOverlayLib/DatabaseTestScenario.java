/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;

import java.io.File;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.*;

import org.nodomain.volkerk.JDBCOverlayLib.JDBC_GenericDB.DB_ENGINE;

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
    
    protected static final String SQLITE_DB = "SqliteTestDB.db";
    
    
    protected Connection getMysqlConn(boolean openDB) throws SQLException
    {
        String connStr = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/";
        
        if (openDB) connStr += MYSQL_DB;
        
        return DriverManager.getConnection(connStr, DB_USER, DB_PASSWD);
    }
    
    protected Connection getSqliteConn() throws SQLException
    {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(DatabaseTestScenario.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        String connStr = "jdbc:sqlite:" + getSqliteFileName();
        
        return DriverManager.getConnection(connStr);
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
    
    protected void prepScenario01(DB_ENGINE t) throws SQLException
    {
        Connection c;
        
        if (t == DB_ENGINE.MYSQL) {
            cleanupMysql();
            c = getMysqlConn(true);
        } else {
            cleanupOutDir();
            assertFalse(sqliteFileExists());
            c = getSqliteConn();
            assertTrue(sqliteFileExists());
        }
        Statement s = c.createStatement();
        
        String aiStr = (t == DB_ENGINE.MYSQL) ? "AUTO_INCREMENT" : "AUTOINCREMENT";
        String nowStr = (t == DB_ENGINE.MYSQL) ? "NOW()" : "date('now')";
        String viewStr = (t == DB_ENGINE.MYSQL) ? "CREATE OR REPLACE VIEW" : "CREATE VIEW IF NOT EXISTS";

        
        s.execute("CREATE TABLE IF NOT EXISTS t1 (id INTEGER NOT NULL PRIMARY KEY " + aiStr + "," +
                " i INT, f DOUBLE, s VARCHAR(40), d DATETIME)");
        
        s.execute("CREATE TABLE IF NOT EXISTS t2 (id INTEGER NOT NULL PRIMARY KEY " + aiStr + "," +
                " i INT, f DOUBLE, s VARCHAR(40), d DATETIME)");
        
        s.execute("INSERT INTO t1 VALUES (NULL, 42, 23.23, 'Hallo', " + nowStr + ")");
        s.execute("INSERT INTO t1 VALUES (NULL, NULL, 666.66, 'Hi', " + nowStr + ")");
        s.execute("INSERT INTO t1 VALUES (NULL, 84, NULL, 'Ho', " + nowStr + ")");
        s.execute("INSERT INTO t1 VALUES (NULL, 84, NULL, 'Hoi', " + nowStr + ")");
        s.execute("INSERT INTO t1 VALUES (NULL, 84, 42.42, 'Ho', " + nowStr + ")");
        
        s.execute(viewStr + " v1 AS SELECT i, f, s FROM t1 WHERE i=84");
        
        c.close();
    }
    
    protected SampleDB getScenario01(DB_ENGINE t) throws SQLException
    {
        prepScenario01(t);
        
        if (t == DB_ENGINE.MYSQL) {
            return new SampleDB(JDBC_GenericDB.DB_ENGINE.MYSQL, "localhost", 3306, "unittest", "unittest", "unittest");
        }
        return new SampleDB(Paths.get(outDir(), SQLITE_DB).toString(), true);
    }
    
    public String getSqliteFileName() {
        return Paths.get(outDir(), SQLITE_DB).toString();
    }
    
    public boolean sqliteFileExists() {
        File f = new File(getSqliteFileName());
        return f.exists();
    }
	
}
