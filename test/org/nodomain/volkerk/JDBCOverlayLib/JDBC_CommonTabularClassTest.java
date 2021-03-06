/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;

import java.sql.SQLException;
import org.junit.Test;
import static org.junit.Assert.*;

import static org.nodomain.volkerk.JDBCOverlayLib.JDBC_GenericDB.DB_ENGINE;

/**
 *
 * @author volker
 */
public class JDBC_CommonTabularClassTest extends DatabaseTestScenario {
    
    @Test
    public void testConstructor() throws SQLException {
        _testConstructor(DB_ENGINE.MYSQL);
        _testConstructor(DB_ENGINE.SQLITE);
    }

    public void _testConstructor(DB_ENGINE engine) throws SQLException {
        
        SampleDB db = getScenario01(engine);
        
        // regular constructor call
        JDBC_CommonTabularClass t = new JDBC_CommonTabularClass(db, "t1", false);
        assertNotNull(t);
        t = new JDBC_CommonTabularClass(db, "v1", true);
        assertNotNull(t);
        
        // calls with wrong parameters
        try
        {
            t = new JDBC_CommonTabularClass(db, "ksdjfhsd", false);
            fail("Tabular constructor accepted wrong parameters!");
        }
        catch (Exception e) {}
        try
        {
            t = new JDBC_CommonTabularClass(db, "t1", true);
            fail("Tabular constructor accepted wrong parameters!");
        }
        catch (Exception e) {}
        try
        {
            t = new JDBC_CommonTabularClass(null, "t1", false);
            fail("Tabular constructor accepted wrong parameters!");
        }
        catch (Exception e) {}
        try
        {
            t = new JDBC_CommonTabularClass(db, "v1", false);
            fail("Tabular constructor accepted wrong parameters!");
        }
        catch (Exception e) {}
    }
	
//----------------------------------------------------------------------------
    
    @Test
    public void testHasColumn() throws SQLException
    {
        _testHasColumn(DB_ENGINE.MYSQL);
        _testHasColumn(DB_ENGINE.SQLITE);
    }
    
    public void _testHasColumn(DB_ENGINE engine) throws SQLException
    {
        SampleDB db = getScenario01(engine);
        JDBC_CommonTabularClass t = new JDBC_CommonTabularClass(db, "t1", false);
        
        assertTrue(t.hasColumn("i"));
        assertFalse(t.hasColumn("sfldk"));
    }
	
//----------------------------------------------------------------------------
    
    @Test
    public void testGetRowMatchesByWhereClause() throws SQLException
    {
        _testGetRowMatchesByWhereClause(DB_ENGINE.SQLITE);
        _testGetRowMatchesByWhereClause(DB_ENGINE.MYSQL);
    }
    
    public void _testGetRowMatchesByWhereClause(DB_ENGINE engine) throws SQLException
    {
        SampleDB db = getScenario01(engine);
        JDBC_CommonTabularClass t = new JDBC_CommonTabularClass(db, "t1", false);

        int i = t.getRowMatchesByWhereClause("i = ?", 84);
        assertTrue(i == 3);
        
        i = t.getRowMatchesByWhereClause("i  > ? and f < ?", 10, 100);
        assertTrue(i == 2);
        
        i = t.getRowMatchesByWhereClause("s = ?", "Ho");
        assertTrue(i == 2);
        
        i = t.getRowMatchesByWhereClause("f is null");
        assertTrue(i == 2);
        
        i = t.getRowMatchesByWhereClause("i > 100");
        assertTrue(i == 0);
    }
	
//----------------------------------------------------------------------------

    @Test
    public void testGetRowMatchesByColumnValue() throws SQLException
    {
        _testGetRowMatchesByColumnValue(DB_ENGINE.SQLITE);
        _testGetRowMatchesByColumnValue(DB_ENGINE.MYSQL);
    }
    
    public void _testGetRowMatchesByColumnValue(DB_ENGINE engine) throws SQLException
    {
        SampleDB db = getScenario01(engine);
        JDBC_CommonTabularClass t = new JDBC_CommonTabularClass(db, "t1", false);

        int i = t.getRowMatchesByColumnValue("i", 84);
        assertTrue(i == 3);
        
        i = t.getRowMatchesByColumnValue("s", "Ho");
        assertTrue(i == 2);
        
        i = t.getRowMatchesByColumnValue("f", null);
        assertTrue(i == 2);
        
        i = t.getRowMatchesByColumnValue("i", 42, "f", 23.23);
        assertTrue(i == 1);
        
    }
	
//----------------------------------------------------------------------------

    @Test
    public void testGetRowsByWhereClause() throws SQLException
    {
        _testGetRowsByWhereClause(DB_ENGINE.SQLITE);
        _testGetRowsByWhereClause(DB_ENGINE.MYSQL);
    }
    
    public void _testGetRowsByWhereClause(DB_ENGINE engine) throws SQLException
    {
        SampleDB db = getScenario01(engine);
        JDBC_CommonTabularClass t = new JDBC_CommonTabularClass(db, "t1", false);

        for (TabRow tr : (TabRowIterator) t.getRowsByWhereClause("i = ?", 42))
        {
            assertTrue(tr.id() == 1);
        }
        
        int cnt=0;
        for (TabRow tr : (TabRowIterator) t.getRowsByWhereClause("s = ?", "Ho"))
        {
            assertTrue( ((tr.id() == 3) || (tr.id() == 5)) );
            cnt++;
        }
        assertTrue(cnt == 2);
        
    }
	
//----------------------------------------------------------------------------

    @Test
    public void testGetRowsByColumnValue() throws SQLException
    {
        _testGetRowsByColumnValue(DB_ENGINE.SQLITE);
        _testGetRowsByColumnValue(DB_ENGINE.MYSQL);
    }
    
    public void _testGetRowsByColumnValue(DB_ENGINE engine) throws SQLException
    {
        SampleDB db = getScenario01(engine);
        JDBC_CommonTabularClass t = new JDBC_CommonTabularClass(db, "t1", false);

        for (TabRow tr : (TabRowIterator) t.getRowsByColumnValue("i", 42, "f", 23.23))
        {
            assertTrue(tr.id() == 1);
        }
        
        int cnt=0;
        for (TabRow tr : (TabRowIterator) t.getRowsByColumnValue("s", "Ho"))
        {
            assertTrue( ((tr.id() == 3) || (tr.id() == 5)) );
            cnt++;
        }
        assertTrue(cnt == 2);
        
    }
	
//----------------------------------------------------------------------------

    @Test
    public void testGetAllRows() throws SQLException
    {
        _testGetAllRows(DB_ENGINE.SQLITE);
        _testGetAllRows(DB_ENGINE.MYSQL);
    }
    
    public void _testGetAllRows(DB_ENGINE engine) throws SQLException
    {
        SampleDB db = getScenario01(engine);
        JDBC_CommonTabularClass t = new JDBC_CommonTabularClass(db, "t1", false);
        
        // iterate over a table
        int expectedId = 1;
        for (TabRow r : (TabRowIterator) t.allRows())
        {
            assertTrue(r.id() == expectedId);
            expectedId++;
        }
        assertTrue(expectedId == 6);
        
        // iterate over a view
        t = new JDBC_CommonTabularClass(db, "v1", true);
        int cnt = 0;
        for (ViewRow r : (ViewRowIterator) t.allRows())
        {
            assertTrue(r.asInt("i") == 84);
            cnt++;
        }
        assertTrue(cnt == 3);
    }

//----------------------------------------------------------------------------

    @Test
    public void testGetNumRow() throws SQLException
    {
        _testGetNumRow(DB_ENGINE.SQLITE);
        _testGetNumRow(DB_ENGINE.MYSQL);
    }
    
    public void _testGetNumRow(DB_ENGINE engine) throws SQLException
    {
        SampleDB db = getScenario01(engine);
        JDBC_CommonTabularClass t = db.t("t1");
        assertTrue(t.getNumRows() == 5);
        
        t = db.t("t2");
        assertTrue(t.getNumRows() == 0);
        
        t = db.v("v1");
        assertTrue(t.getNumRows() == 3);
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
    
}