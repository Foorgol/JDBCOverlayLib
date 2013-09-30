/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.nodomain.volkerk.JDBCOverlayLib.DatabaseTestScenario.SQLITE_DB;
import static org.nodomain.volkerk.JDBCOverlayLib.JDBC_GenericDB.DB_ENGINE;

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
        SampleDB db = new SampleDB(DB_ENGINE.MYSQL, "localhost", 3306, "unittest", "unittest", "unittest");
        assertNotNull(db);
        db.close();
        
        // open using default parameters
        db = new SampleDB(DB_ENGINE.MYSQL, null, 0, "unittest", "unittest", "unittest");
        assertNotNull(db);
        db.close();
        
        // open non-existing db
        try
        {
            db = new SampleDB(DB_ENGINE.MYSQL, "sdfjhsdf", 3306, "unittest", "unittest", "unittest");
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
    public void testConstructorSQlite() throws SQLException {
        // delete all stale SQLite test database
        cleanupOutDir();
        
        // create the db file path
        File dbFile = new File(outDir(), SQLITE_DB);
        System.err.println("Outdir = " + outDir());
        System.err.println("dbFile = " + dbFile.toString());
        
        // create a new, empty db-file from scratch
        assertFalse(dbFile.exists());
        SampleDB db = new SampleDB(dbFile.getAbsolutePath(), true);
        assertNotNull(db);
        db.close();
        assertTrue(dbFile.exists());
        
        // create an existing db file
        db = null;
        assertTrue(dbFile.exists());
        db = new SampleDB(dbFile.getAbsolutePath(), false);
        assertNotNull(db);
        db.close();        
        
        // open non-existing db
        try
        {
            db = new SampleDB("skdhfskdf", false);
            fail("Could open non-existing database");
        }
        catch (Exception e) {}
        
        // make sure tables and views have been created
        Connection c = getSqliteConn();
        ResultSet rs = c.createStatement().executeQuery("SELECT * FROM sqlite_master WHERE type='table'");
        assertTrue(rs.next());
        assertTrue(rs.getString(2).equals("t1"));
        
        rs = c.createStatement().executeQuery("SELECT * FROM sqlite_master WHERE type='view'");
        assertTrue(rs.next());
        assertTrue(rs.getString(2).equals("v1"));
        assertFalse(rs.next());
    }
	
//----------------------------------------------------------------------------
    
    @Test
    public void testGetTableNames() throws SQLException {
        _testGetTableNames(DB_ENGINE.MYSQL);
        _testGetTableNames(DB_ENGINE.SQLITE);
    }
    
    public void _testGetTableNames(DB_ENGINE t) throws SQLException
    {
        SampleDB db = getScenario01(t);
        
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
    public void testGetViewNames() throws SQLException {
        _testGetViewNames(DB_ENGINE.MYSQL);
        _testGetViewNames(DB_ENGINE.SQLITE);
    }

    public void _testGetViewNames(DB_ENGINE t) throws SQLException
    {
        SampleDB db = getScenario01(t);
        
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
        _testHasViewOrTable(DB_ENGINE.MYSQL);
        _testHasViewOrTable(DB_ENGINE.SQLITE);
    }
    
    public void _testHasViewOrTable(DB_ENGINE t) throws SQLException
    {
        SampleDB db = getScenario01(t);
        
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