/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author volker
 */
public class TabRowTest extends DatabaseTestScenario {
    
    @Test
    public void testConstructor() throws SQLException {
        
        SampleDB db = getScenario01();
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
        
        ResultSet rs = getMysqlConn(true).createStatement().executeQuery("SELECT * FROM t1 WHERE id=6");
        assertTrue(rs.next());
        assertTrue(rs.getInt(2) == 100);
        rs.getDouble(3);
        assertTrue(rs.wasNull());
        assertTrue(rs.getString(4).equals("test"));
    }
    
//----------------------------------------------------------------------------

    @Test
    public void testGetColumnAsString() throws SQLException
    {
        SampleDB db = getScenario01();
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
        SampleDB db = getScenario01();
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
        SampleDB db = getScenario01();
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