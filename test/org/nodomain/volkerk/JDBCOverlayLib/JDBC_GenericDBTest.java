/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author volker
 */
public class JDBC_GenericDBTest extends DatabaseTestScenario {
    

    @Test
    public void testConstructor() throws SQLException {
        cleanupMysql();
        
        // open existing database using explicit parameters
        SampleDB db = new SampleDB(JDBC_GenericDB.DB_ENGINE.MYSQL, "localhost", 3306, "unittest", "unittest", "unittest");
        assertNotNull(db);
        db.close();
        
        // open using default parameters
        db = new SampleDB(JDBC_GenericDB.DB_ENGINE.MYSQL, null, 0, "unittest", "unittest", "unittest");
        assertNotNull(db);
        db.close();
        
        // open non-existing db
        try
        {
            db = new SampleDB(JDBC_GenericDB.DB_ENGINE.MYSQL, "sdfjhsdf", 3306, "unittest", "unittest", "unittest");
            fail("Could open non-existing database");
        }
        catch (Exception e) {}
        
        // make sure tables have been created
        Connection c = getMysqlConn(true);
        ResultSet rs = c.createStatement().executeQuery("SHOW TABLES");
        assertTrue(rs.first());
        assertTrue(rs.getString(1).equals("t1"));
    }
}