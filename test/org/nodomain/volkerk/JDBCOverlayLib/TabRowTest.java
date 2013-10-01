/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import org.junit.Test;
import static org.junit.Assert.*;

import static org.nodomain.volkerk.JDBCOverlayLib.JDBC_GenericDB.DB_ENGINE;

/**
 *
 * @author volker
 */
public class TabRowTest extends DatabaseTestScenario {
    
    @Test
    public void testConstructor() throws SQLException {
        _testConstructor(DB_ENGINE.SQLITE);
        _testConstructor(DB_ENGINE.MYSQL);
    }
    
    public void _testConstructor(DB_ENGINE engine) throws SQLException {
        
        SampleDB db = getScenario01(engine);
        JDBC_Tab t = db.t("t1");
        TabRow r = null;

        //
        // Constructor Type 1: db, tabname, rowId
        //
        r = new TabRow(db, "t1", 2);
        assertNotNull(r);
        assertTrue(r.id() == 2);
        try
        {
            r = new TabRow(db, "sdfd", 2);
            fail("Could get row from non-existing table!");
        }
        catch (Exception e) {}
        try
        {
            r = new TabRow(db, "t1", 200);
            fail("Could get row from non-existing ID!");
        }
        catch (Exception e) {}
        
        //
        // Constructor Type 2a: db, tabname, column/value pair
        //
        r = new TabRow(db, "t1", "i", 84);
        assertNotNull(r);
        assertTrue(r.id() == 3);
        r = new TabRow(db, "t1", "i", null, "s", "Hi");
        assertNotNull(r);
        assertTrue(r.id() == 2);
        
        //
        // Constructor Type 2b: db, tabname, where clause, parameters
        //
        r = new TabRow(db, "t1", "i > ?", 50);
        assertNotNull(r);
        assertTrue(r.id() == 3);
        r = new TabRow(db, "t1", "i IS NULL AND f > ? AND s = ?", 600, "Hi");
        assertNotNull(r);
        assertTrue(r.id() == 2);
        
        //
        // Constructor Type 3: create a new row
        //
        HashMap<String, Object> cv = new HashMap<>();
        cv.put("i", 100);
        cv.put("s", "test");
        r = new TabRow(db, "t1", cv);
        assertNotNull(r);
        assertTrue(r.id() == 6);
        
        Connection c = (engine == DB_ENGINE.MYSQL) ? getMysqlConn(true) : getSqliteConn();
        ResultSet rs = c.createStatement().executeQuery("SELECT * FROM t1 WHERE id=6");
        assertTrue(rs.next());
        assertTrue(rs.getInt(2) == 100);
        rs.getDouble(3);
        assertTrue(rs.wasNull());
        assertTrue(rs.getString(4).equals("test"));
        c.close();
    }
    
//----------------------------------------------------------------------------

    @Test
    public void testGetColumnAsString() throws SQLException
    {
        _testGetColumnAsString(DB_ENGINE.SQLITE);
        _testGetColumnAsString(DB_ENGINE.MYSQL);
    }
    
    public void _testGetColumnAsString(DB_ENGINE engine) throws SQLException
    {
        SampleDB db = getScenario01(engine);
        TabRow r = new TabRow(db, "t1", 1);
        
        assertTrue(r.c("s").equals("Hallo"));
        assertTrue(r.c("i").equals("42"));
        
        r = new TabRow(db, "t1", 2);
        assertNull(r.c("i"));
        
        try
        {
            r.c("kdjf");
            fail("Could access non-existing column!");
        }
        catch (Exception e) {}
    }

//----------------------------------------------------------------------------

    @Test
    public void testGetColumnAsInt() throws SQLException
    {
        _testGetColumnAsInt(DB_ENGINE.SQLITE);
        _testGetColumnAsInt(DB_ENGINE.MYSQL);
    }
    
    public void _testGetColumnAsInt(DB_ENGINE engine) throws SQLException
    {
        SampleDB db = getScenario01(engine);
        TabRow r = new TabRow(db, "t1", 1);
        
        assertTrue(r.asInt("i") == 42);
        
        r = new TabRow(db, "t1", 2);
        assertNull(r.asInt("i"));
        
        try
        {
            r.asInt("kdjf");
            fail("Could access non-existing column!");
        }
        catch (Exception e) {}
        
        try
        {
            r.asInt("s");
            fail("Could cast non-castable data...");
        }
        catch (Exception e) {}
    }

//----------------------------------------------------------------------------

    @Test
    public void testGetColumnAsDouble() throws SQLException
    {
        _testGetColumnAsDouble(DB_ENGINE.SQLITE);
        _testGetColumnAsDouble(DB_ENGINE.MYSQL);
    }
    
    public void _testGetColumnAsDouble(DB_ENGINE engine) throws SQLException
    {
        SampleDB db = getScenario01(engine);
        TabRow r = new TabRow(db, "t1", 1);
        
        assertTrue(r.asDouble("f") == 23.23);
        assertTrue(r.asDouble("i") == 42.0);
        
        r = new TabRow(db, "t1", 3);
        assertNull(r.asDouble("f"));
        
        try
        {
            r.asDouble("kdjf");
            fail("Could access non-existing column!");
        }
        catch (Exception e) {}
        
        try
        {
            r.asDouble("s");
            fail("Could cast non-castable data...");
        }
        catch (Exception e) {}
    }

//----------------------------------------------------------------------------

    @Test
    public void testTabRowIsNotCaching() throws SQLException
    {
        _testTabRowIsNotCaching(DB_ENGINE.SQLITE);
        _testTabRowIsNotCaching(DB_ENGINE.MYSQL);
    }
    
    public void _testTabRowIsNotCaching(DB_ENGINE engine) throws SQLException
    {
        SampleDB db = getScenario01(engine);
        TabRow r = new TabRow(db, "t1", 1);
        
        assertTrue(r.asInt("i") == 42);
        
        // manipulate that row
        //
        // If we're using MySQL, we can do that over a separate connection.
        // With SQLite, we have to use same connection (because its mode is
        // set to "SHARED" by the JDBC driver; SHARED prevents other connections
        // from writing) but use a "fresh" tab row object to achieve at least some
        // virtual concurrency
        if (engine == DB_ENGINE.MYSQL) {
            Connection c = (engine == DB_ENGINE.MYSQL) ? getMysqlConn(true) : getSqliteConn();
            c.createStatement().executeUpdate("UPDATE t1 SET i=777 WHERE id=1");
            c.close();
        } else {
            TabRow r1 = new TabRow(db, "t1", 1);
            r1.updateColumn("i", 777);
        }
        
        // make sure the row reflects that change
        assertTrue(r.asInt("i") == 777);
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


//----------------------------------------------------------------------------


}