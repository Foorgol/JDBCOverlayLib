/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.sql.rowset.CachedRowSet;

/**
 * A class that represents the common functions for tables and views in a JDDC DB
 * 
 * @author volker
 */
public class JDBC_CommonTabularClass {
    /**
     * the name of the associated table or view
     */
    protected String tabName;
    
    /**
     * the handle to the (parent) database
     */
    protected JDBC_GenericDB db;
    
    /**
     * a tag whether we are a view or a tab
     */
    protected boolean isView;
    
//----------------------------------------------------------------------------

    /**
     * Basic constructor of a table or view
     * 
     * @param _db the reference to the database instance for this table / view
     * @param _tabName the name of the table / view
     * @param _isView true if name refers to a view and "false" for tables
     */
    public JDBC_CommonTabularClass(JDBC_GenericDB _db, String _tabName, boolean _isView)
    {
        // make sure that the given table name actually exists
        if (!(_db.hasTableOrView(_tabName, _isView)))
        {
            throw new IllegalArgumentException("Invalid view or table name: " + _tabName);
        }
        
        db = _db;
        tabName = _tabName;
        isView = _isView;
    }

//----------------------------------------------------------------------------

    /**
     * Returns a list of all column definition for this !!SQLITE!! table
     * 
     * @return an ArrayList of ColInfo objects containing all column info
     */
    public ArrayList<ColInfo> allColDefs_SQLite()
    {
        CachedRowSet rs = null;
        
        try
        {
            rs = db.execContentQuery("PRAGMA table_info(" + tabName + ")");
        }
        catch (Exception e)
        {
            db.genericExceptionHandler(e, "TabRow.allColDefs PRAGMA\n");
        }
        
        ArrayList<ColInfo> result = new ArrayList<>();
        if (rs == null) return result;
        
        try
        {
            while (rs.next())
            {
                result.add(new ColInfo(rs.getInt(1),  // column number
                        rs.getString(2),  // column name
                        rs.getString(3)));  // column type
            }
            rs.close();
        }
        catch (Exception e)
        {
            db.genericExceptionHandler(e, "TabRow.allColDefs READ\n");
        }
        
        return result;
    }

//----------------------------------------------------------------------------

    /**
     * Returns a list of all column definition for this !!MYSQL!! table
     * 
     * @return an ArrayList of ColInfo objects containing all column info
     */
    public ArrayList<ColInfo> allColDefs_MySQL()
    {
        CachedRowSet rs = null;
        
        try
        {
            rs = db.execContentQuery("DESCRIBE " + tabName);
        }
        catch (Exception e)
        {
            db.genericExceptionHandler(e, "TabRow.allColDefs Mysql\n");
        }
        
        ArrayList<ColInfo> result = new ArrayList<>();
        if (rs == null) return result;
        
        try
        {
            int i=1;
            while (rs.next())
            {
                result.add(new ColInfo(i,  // column number
                        rs.getString(1),  // column name
                        rs.getString(2)));  // column type
                i++;
            }
            rs.close();
        }
        catch (Exception e)
        {
            db.genericExceptionHandler(e, "TabRow.allColDefs READ\n");
        }
        
        return result;
    }


//----------------------------------------------------------------------------

    /**
     * Returns a list of all column definition for this table
     * 
     * @return an ArrayList of ColInfo objects containing all column info
     */
    public ArrayList<ColInfo> allColDefs()
    {
        if (db.getEngineType() == JDBC_GenericDB.DB_ENGINE.SQLITE) return allColDefs_SQLite();
        else return allColDefs_MySQL();
    }

//----------------------------------------------------------------------------
    
    /**
     * Returns a string with the SQL type of a column
     * 
     * @param name is the column name of the column
     * @return the SQL type as capitalized string or null if the name was invalid
     */
    public String getColType(String name)
    {
        for (ColInfo ci : allColDefs())
        {
            if (ci.name().equals(name)) {
                return ci.type();
            }
        }
        return null;
    }

//----------------------------------------------------------------------------
    
    /**
     * Resolves a column ID to a column name
     * 
     * @param cid is the column id of the column
     * @return the column name as a string or null of the ID was invalid
     */
    public String cid2name(int cid)
    {
        for (ColInfo ci : allColDefs())
        {
            if (ci.cid() == cid) return ci.name();
        }
        return null;
    }

//----------------------------------------------------------------------------

    /**
     * Determines whether a specific column exists in the table
     * 
     * @param colName name of the column to check
     * @return true if a column of that name exists
     */
    public boolean hasColumn(String colName)
    {
        for (ColInfo ci : allColDefs())
        {
            if (ci.name().equals(colName)) return true;
        }
        return false;
    }

//----------------------------------------------------------------------------

    /**
     * Returns the number of rows which match a certain where clause
     * 
     * @param whereClause is the where clause to be matched
     * @return the number of matching rows
     */
    public int getRowMatchesByWhereClause(String whereClause, Object ... params)
    {
        String sql = "SELECT count(*) FROM " + tabName + " WHERE " + whereClause;
        Integer result = null;
        db.log(sql);
        try
        {
            result = db.execScalarQueryInt(sql, params);
            db.log(result);
        }
        catch (Exception e)
        {
            db.genericExceptionHandler(e, "TabRow.getRowMatchesByWhereClause\n",
                    "SQL = ", sql);
        }
        
        if (result != null) return result.intValue();
        return 0;
    }

//----------------------------------------------------------------------------

    /**
     * Returns the number of rows that match certain values in certain columns
     * 
     * Multiple column/value pairs are concatenated using AND
     * 
     * @param args a list of Strings / Objects for column / value pairs
     * @return the number of matches
     */
    public int getRowMatchesByColumnValue(Object ... args)
    {
        ArrayList<Object> whereStuff = new ArrayList<>(Arrays.asList(helper.prepWhereClause(args)));
        
        String clause = whereStuff.get(0).toString();
        whereStuff.remove(0);
        
        if ((whereStuff.size() == 1) && (whereStuff.get(0) == null))
        {
            return getRowMatchesByWhereClause(clause);
        }
        
        Object[] params = whereStuff.toArray();
        return getRowMatchesByWhereClause(clause, params);
    }

//----------------------------------------------------------------------------

    /**
     * Returns an iterator over all rows that match a certain where clause
     * 
     * @param whereClause is the where clause to be matched
     * @return an iterable object yielding TabRow instances
     */
    public Iterable<?> getRowsByWhereClause(String whereClause, Object ... params)
    {
        String sql = "SELECT * FROM " + tabName + " WHERE " + whereClause;
        CachedRowSet rs = null;
        try
        {
            rs = db.execContentQuery(sql, params);
        }
        catch (Exception e)
        {
            db.genericExceptionHandler(e, "TabRow.getRowsByWhereClause\n",
                    "SQL = ", sql);
        }
        
        return isView ? new ViewRowIterator(rs) : new TabRowIterator(db, tabName, rs);
    }
    
//----------------------------------------------------------------------------
    
    /**
     * Returns an iterator over all rows that match certain values in certain columns
     * 
     * Multiple column/value pairs are concatenated using AND
     * 
     * @param args a list of Strings / Objects for column / value pairs
     * @return the iterator for the rows or null
     */
    public Iterable<?> getRowsByColumnValue(Object ... args)
    {
        ArrayList<Object> whereStuff = new ArrayList<>(Arrays.asList(helper.prepWhereClause(args)));
        
        String clause = whereStuff.get(0).toString();
        whereStuff.remove(0);
        
        if ((whereStuff.size() == 1) && (whereStuff.get(0) == null))
        {
            return getRowsByWhereClause(clause);
        }
        
        Object[] params = whereStuff.toArray();
        return getRowsByWhereClause(clause, params);
    }
    
//----------------------------------------------------------------------------

    public Iterable<?> allRows()
    {
        String sql = "SELECT * FROM " + tabName;
        CachedRowSet rs = null;
        try
        {
            rs = db.execContentQuery(sql);
        }
        catch (Exception e)
        {
            db.genericExceptionHandler(e, "TabRow.getRowsByWhereClause\n",
                    "SQL = ", sql);
        }
        
        return isView ? new ViewRowIterator(rs) : new TabRowIterator(db, tabName, rs);
        
    }

//----------------------------------------------------------------------------

    public int getNumRows() throws SQLException
    {
        String sql = "SELECT count(*) FROM " + tabName;
        return db.execScalarQueryInt(sql);
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

}
