/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author volker
 */
public class JDBC_GenericDBTest extends DatabaseTestScenario {
    
//----------------------------------------------------------------------------
    
    @Test
    public void testConstructorMysql() throws SQLException {
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
        
        // make sure tables and views have been created
        Connection c = getMysqlConn(true);
        ResultSet rs = c.createStatement().executeQuery("SHOW TABLES");
        assertTrue(rs.first());
        assertTrue(rs.getString(1).equals("t1"));
        assertTrue(rs.next());
        assertTrue(rs.getString(1).equals("t2"));
        assertTrue(rs.next());
        assertTrue(rs.getString(1).equals("v1"));
        assertFalse(rs.next());
    }
	
//----------------------------------------------------------------------------
    
    @Test
    public void testGetTableNames() throws SQLException
    {
        SampleDB db = getScenario01();
        
        ArrayList<String> tabs = new ArrayList<>();
        tabs.add("t1");
        tabs.add("t2");
       
        for (String tabName : db.allTableNames())
        {
            if (tabs.contains(tabName))
            {
                // we found an expected table.
                // remove it from the list
                tabs.remove(tabName);
            }
            else
            {
                // we found a table that shouldn't be there
                fail("Unexpected table in database: " + tabName);
            }
        }
        
        assertTrue(tabs.isEmpty());
        
        db.close();

    }
	
//----------------------------------------------------------------------------
    
    @Test
    public void testGetViewNames() throws SQLException
    {
        SampleDB db = getScenario01();
        
        ArrayList<String> tabs = new ArrayList<>();
        tabs.add("v1");
       
        for (String tabName : db.allViewNames())
        {
            if (tabs.contains(tabName))
            {
                // we found an expected table.
                // remove it from the list
                tabs.remove(tabName);
            }
            else
            {
                // we found a table that shouldn't be there
                fail("Unexpected view in database: " + tabName);
            }
        }
        
        assertTrue(tabs.isEmpty());
        
        db.close();

    }
	
//----------------------------------------------------------------------------
    
    @Test
    public void testHasViewOrTable() throws SQLException
    {
        SampleDB db = getScenario01();
        
        assertTrue(db.hasTable("t1"));
        assertTrue(db.hasTable("t2"));
        assertFalse(db.hasTable("tsdfkjsdf"));
        assertTrue(db.hasView("v1"));
        assertFalse(db.hasTable("sldfjsdf"));
    }
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
    
}