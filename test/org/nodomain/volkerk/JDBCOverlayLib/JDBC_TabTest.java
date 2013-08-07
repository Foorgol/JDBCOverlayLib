/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author volker
 */
public class JDBC_TabTest extends DatabaseTestScenario {
    
    @Test
    public void testInsert() throws SQLException
    {
        SampleDB db = getScenario01();
        JDBC_Tab t = db.t("t1");

        assertTrue(t.getNumRows() == 5);
        
        // normal insert
        HashMap<String, Object> cv = new HashMap<>();
        cv.put("i", 123);
        cv.put("f", 56.78);
        cv.put("s", "volker");
        int newId = t.insertRow(cv);
        assertTrue(newId == 6);
        ResultSet rs = getMysqlConn(true).createStatement().executeQuery("SELECT * FROM t1 WHERE id=6");
        assertTrue(rs.next());
        assertTrue(rs.getInt(2) == 123);
        assertTrue(rs.getDouble(3) == 56.78);
        assertTrue(rs.getString(4).equals("volker"));
        
        // insert empty row
        newId = t.insertRow();
        assertTrue(newId == 7);
        cv.clear();
        newId = t.insertRow(cv);
        assertTrue(newId == 8);
        
        
        // normal insert with column / value pairs
        newId = t.insertRow("i", 124, "f", 12.34, "s", "Lala");
        assertTrue(newId == 9);
        rs = getMysqlConn(true).createStatement().executeQuery("SELECT * FROM t1 WHERE id=9");
        assertTrue(rs.next());
        assertTrue(rs.getInt(2) == 124);
        assertTrue(rs.getDouble(3) == 12.34);
        assertTrue(rs.getString(4).equals("Lala"));
    }

//----------------------------------------------------------------------------

    @Test
    public void testUpdate() throws SQLException
    {
        SampleDB db = getScenario01();
        JDBC_Tab t = db.t("t1");
        
        // normal update
        HashMap<String, Object> cv = new HashMap<>();
        cv.put("i", 43);
        cv.put("s", 666);
        t.updateRow(1, cv);
        
        ResultSet rs = getMysqlConn(true).createStatement().executeQuery("SELECT * FROM t1 WHERE id=1");
        assertTrue(rs.next());
        assertTrue(rs.getInt(2) == 43);
        assertTrue(rs.getDouble(3) == 23.23);
        assertTrue(rs.getString(4).equals("666"));
        
        // normal update with column / value pairs
        t.updateRow(2, "i", 124, "f", 12.34, "s", "Lala");
        rs = getMysqlConn(true).createStatement().executeQuery("SELECT * FROM t1 WHERE id=2");
        assertTrue(rs.next());
        assertTrue(rs.getInt(2) == 124);
        assertTrue(rs.getDouble(3) == 12.34);
        assertTrue(rs.getString(4).equals("Lala"));
    }

//----------------------------------------------------------------------------

    @Test
    public void testGetSingleRowByWhereClause() throws SQLException
    {
        SampleDB db = getScenario01();
        JDBC_Tab t = db.t("t1");
        
        // get existing row
        TabRow r = t.getSingleRowByWhereClause("i = ?", 84);
        assertNotNull(r);
        assertTrue(r.id() == 3);
        
        // get row with null value
        r = t.getSingleRowByWhereClause("f IS NULL");
        assertNotNull(r);
        assertTrue(r.id() == 3);
        
        // get non-existing row
        r = t.getSingleRowByWhereClause("i = 3947853");
        assertNull(r);
    }

//----------------------------------------------------------------------------

    @Test
    public void testGetSingleRowByColumnValue() throws SQLException
    {
        SampleDB db = getScenario01();
        JDBC_Tab t = db.t("t1");
        
        // get existing row
        TabRow r = t.getSingleRowByColumnValue("i", 84);
        assertNotNull(r);
        assertTrue(r.id() == 3);
        
        // get row with null value
        r = t.getSingleRowByColumnValue("f", null);
        assertNotNull(r);
        assertTrue(r.id() == 3);
        
        // get non-existing row
        r = t.getSingleRowByColumnValue("i", 3947853);
        assertNull(r);
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

}