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
import java.util.HashMap;

import static org.nodomain.volkerk.JDBCOverlayLib.JDBC_GenericDB.DB_ENGINE;

/**
 *
 * @author volker
 */
public class JDBC_TabTest extends DatabaseTestScenario {
    
    @Test
    public void testInsert() throws SQLException
    {
        _testInsert(DB_ENGINE.SQLITE);
        _testInsert(DB_ENGINE.MYSQL);
    }
    
    public void _testInsert(DB_ENGINE engine) throws SQLException
    {
        SampleDB db = getScenario01(engine);
        JDBC_Tab t = db.t("t1");

        assertTrue(t.getNumRows() == 5);
        
        // normal insert
        HashMap<String, Object> cv = new HashMap<>();
        cv.put("i", 123);
        cv.put("f", 56.78);
        cv.put("s", "volker");
        int newId = t.insertRow(cv);
        assertTrue(newId == 6);
        Connection c = (engine == DB_ENGINE.MYSQL) ? getMysqlConn(true) : getSqliteConn();
        ResultSet rs = c.createStatement().executeQuery("SELECT * FROM t1 WHERE id=6");
        assertTrue(rs.next());
        assertTrue(rs.getInt(2) == 123);
        assertTrue(rs.getDouble(3) == 56.78);
        assertTrue(rs.getString(4).equals("volker"));
        c.close();
        
        // insert empty row
        newId = t.insertRow();
        assertTrue(newId == 7);
        cv.clear();
        newId = t.insertRow(cv);
        assertTrue(newId == 8);
        
        
        // normal insert with column / value pairs
        newId = t.insertRow("i", 124, "f", 12.34, "s", "Lala");
        assertTrue(newId == 9);
        c = (engine == DB_ENGINE.MYSQL) ? getMysqlConn(true) : getSqliteConn();
        rs = c.createStatement().executeQuery("SELECT * FROM t1 WHERE id=9");
        assertTrue(rs.next());
        assertTrue(rs.getInt(2) == 124);
        assertTrue(rs.getDouble(3) == 12.34);
        assertTrue(rs.getString(4).equals("Lala"));
        c.close();
    }

//----------------------------------------------------------------------------

    @Test
    public void testUpdate() throws SQLException
    {
        _testUpdate(DB_ENGINE.SQLITE);
        _testUpdate(DB_ENGINE.MYSQL);
    }
    
    public void _testUpdate(DB_ENGINE engine) throws SQLException
    {
        SampleDB db = getScenario01(engine);
        JDBC_Tab t = db.t("t1");
        
        // normal update
        HashMap<String, Object> cv = new HashMap<>();
        cv.put("i", 43);
        cv.put("s", 666);
        t.updateRow(1, cv);
        
        Connection c = (engine == DB_ENGINE.MYSQL) ? getMysqlConn(true) : getSqliteConn();
        ResultSet rs = c.createStatement().executeQuery("SELECT * FROM t1 WHERE id=1");
        assertTrue(rs.next());
        assertTrue(rs.getInt(2) == 43);
        assertTrue(rs.getDouble(3) == 23.23);
        assertTrue(rs.getString(4).equals("666"));
        c.close();
        
        // normal update with column / value pairs
        t.updateRow(2, "i", 124, "f", 12.34, "s", "Lala");
        c = (engine == DB_ENGINE.MYSQL) ? getMysqlConn(true) : getSqliteConn();
        rs = c.createStatement().executeQuery("SELECT * FROM t1 WHERE id=2");
        assertTrue(rs.next());
        assertTrue(rs.getInt(2) == 124);
        assertTrue(rs.getDouble(3) == 12.34);
        assertTrue(rs.getString(4).equals("Lala"));
        c.close();
    }

//----------------------------------------------------------------------------

    @Test
    public void testGetSingleRowByWhereClause() throws SQLException
    {
        _testGetSingleRowByWhereClause(DB_ENGINE.SQLITE);
        _testGetSingleRowByWhereClause(DB_ENGINE.MYSQL);
    }
    
    public void _testGetSingleRowByWhereClause(DB_ENGINE engine) throws SQLException
    {
        SampleDB db = getScenario01(engine);
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
        _testGetSingleRowByColumnValue(DB_ENGINE.SQLITE);
        _testGetSingleRowByColumnValue(DB_ENGINE.MYSQL);
    }
    
    public void _testGetSingleRowByColumnValue(DB_ENGINE engine) throws SQLException
    {
        SampleDB db = getScenario01(engine);
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

    @Test
    public void testDeleteRowsByColumnValue() throws SQLException
    {
        _testDeleteRowsByColumnValue(DB_ENGINE.SQLITE);
        _testDeleteRowsByColumnValue(DB_ENGINE.MYSQL);
    }
    
    public void _testDeleteRowsByColumnValue(DB_ENGINE engine) throws SQLException
    {
        SampleDB db = getScenario01(engine);
        JDBC_Tab t = db.t("t1");
        
        assertTrue(t.getNumRows() == 5);
        int n = t.deleteRowsByColumnValue("i", 84);
        assertTrue(n == 3);
        assertTrue(t.getNumRows() == 2);
        
        db = getScenario01(engine);
        t = db.t("t1");
        assertTrue(t.getNumRows() == 5);
        n = t.deleteRowsByColumnValue("s", "Hoi", "f", null);
        assertTrue(n == 1);
        assertTrue(t.getNumRows() == 4);
    }

//----------------------------------------------------------------------------

    @Test
    public void testDeleteRowsByWhereClause() throws SQLException
    {
        _testDeleteRowsByWhereClause(DB_ENGINE.SQLITE);
        _testDeleteRowsByWhereClause(DB_ENGINE.MYSQL);
    }
    
    public void _testDeleteRowsByWhereClause(DB_ENGINE engine) throws SQLException
    {
        SampleDB db = getScenario01(engine);
        JDBC_Tab t = db.t("t1");
        
        assertTrue(t.getNumRows() == 5);
        int n = t.deleteRowsByWhereClause("i = ?", 84);
        assertTrue(n == 3);
        assertTrue(t.getNumRows() == 2);
        
        db = getScenario01(engine);
        t = db.t("t1");
        assertTrue(t.getNumRows() == 5);
        n = t.deleteRowsByWhereClause("s = ? AND f IS NULL", "Hoi");
        assertTrue(n == 1);
        assertTrue(t.getNumRows() == 4);
    }

//----------------------------------------------------------------------------


//----------------------------------------------------------------------------


//----------------------------------------------------------------------------


//----------------------------------------------------------------------------


//----------------------------------------------------------------------------


//----------------------------------------------------------------------------


//----------------------------------------------------------------------------

}