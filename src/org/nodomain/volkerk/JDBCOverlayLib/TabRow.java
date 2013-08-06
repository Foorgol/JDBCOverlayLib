/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import javax.sql.rowset.CachedRowSet;

/**
 *
 * @author volker
 */
/**
 * A class representing a single data row in a table (not in a view!)
 * 
 * @author volker
 */
public class TabRow {
    /**
     * the name of the associated table containing the row
     */
    protected String tabName;
    
    /**
     * the handle to the (parent) database
     */
    protected JDBC_GenericDB db;
    
    /**
     * the unique ID of the row
     */
    protected int rowId;
 
//----------------------------------------------------------------------------

    /**
     * Constructor for a known rowID
     * 
     * @param _db the associated database instance
     * @param _tabName the associated table name
     * @param _rowId 
     */
    public TabRow (JDBC_GenericDB _db, String _tabName, int _rowId)
    {
        if (!(doInit(_db, _tabName, "id=" + Integer.toString(_rowId))))
        {
            throw new IllegalArgumentException("TabRow constructor: table " +
                    _tabName + " has no ID " + Integer.toString(_rowId));
        }
    }

//----------------------------------------------------------------------------

    /**
     * Constructor for the first row in the table that matches a column/value pair or a custom where clause.
     * 
     * If the first elements of "args" is a column name, then args is interpreted
     * as a set of column/value pairs which shall be AND-concatenated.
     * 
     * If the first element of "args" is NOT a column name, then the first element
     * is interpreted as a where clause and all subsequent elements are treated as
     * parameters ("?") to that where clause
     * 
     * Throws an exception if the requested row doesn't exist
     * 
     * @param _db the associated database instance
     * @param _tabName the associated table name
     * @param args where clause and/or column/value pairs
     */
    public TabRow(JDBC_GenericDB _db, String _tabName, Object ... args)
    {
        if ((args == null) || (args.length == 0))
        {
            throw new IllegalArgumentException("Arguments can't be null or empty!");
        }
        
        if ((!_db.hasTable(_tabName)))
        {
            throw new IllegalArgumentException("Invalid table name for tab row instanciation!");
        }
        
        // if the first "args" element is a column name, convert args
        // into an array with a WHERE-clause at index zero and parameters in
        // the following positions
        String first = args[0].toString();
        if (_db.t(_tabName).hasColumn(first))
        {
            args = helper.prepWhereClause(args);
        };
        
        ArrayList<Object> whereStuff = new ArrayList<>(Arrays.asList(args));
        
        String clause = whereStuff.get(0).toString();
        whereStuff.remove(0);
        
        Object[] params = whereStuff.toArray();
        
        if ((whereStuff.size() == 1) && (whereStuff.get(0) == null))
        {
            params = null;
        }
        
        if (!(doInit(_db, _tabName, clause, params)))
        {
            throw new IllegalArgumentException("TabRow constructor: table " +
                    _tabName + " has no matching row ");
        }
    }

//----------------------------------------------------------------------------

    /**
     * Inserts a new row into the table and returns the TabRow instance for that
     * 
     * @param _db the associated database instance
     * @param _tabName the associated table name
     * @param colVal a Hashmap with maps column names to column contents
     */
    public TabRow(JDBC_GenericDB _db, String _tabName, HashMap<String, Object> colVal)
    {
        int newId = _db.t(_tabName).insertRow(colVal);
        doInit(_db, _tabName, "id=" + String.valueOf(newId));
    }

//----------------------------------------------------------------------------

    /**
     * Called by the various constructors to finally determine the row ID
     * 
     * Implies a check whether a already provided row ID is valid by querying
     * the ID again. Thus, only valid IDs can "survive" the constructor.
     * This function already sets the rowId so the object initialization is
     * complete when this function exits.
     * 
     * @param _db the DB instance
     * @param _tabName the table name of the to-be-instanciated row
     * @param whereClause the where clause which uniquely defines the row
     * @param params are values for possible placeholders in the where clause
     * @return true if the initialization was successfull
     */
    protected final boolean doInit(JDBC_GenericDB _db, String _tabName, String whereClause, Object ... params)
    {
        db = _db;
        tabName = _tabName;
        
        // make a query to determine the row ID
        String sql = "SELECT id FROM " + tabName + " WHERE " + whereClause;
        
        Integer result = null;
        
        try {
            result = db.execScalarQueryInt(sql, params);
        }
        catch (Exception e)
        {
            //db.genericExceptionHandler(e, "TabRow DoInit failed");
            return false;
        }
        
        if (result == null)
        {
            rowId = -1;  // to be on the safe side
            return false; // signal to the constructor to throw exception
        }
        
        rowId = result.intValue();
        return true;
    }

//----------------------------------------------------------------------------

    public int id()
    {
        return rowId;
    }

//----------------------------------------------------------------------------

    protected CachedRowSet getColumnRowSet(String colName) throws SQLException
    {
        String sql = "SELECT " + colName + " FROM " + tabName + " WHERE id=" + rowId;
        
        CachedRowSet rs = db.execContentQuery(sql);
        if (!(rs.first()))
        {
            throw new IllegalStateException("Row with ID " + rowId + " in table " + tabName +
                    " has been deleted or has no column " + colName + "!");
        }
        
        return rs;
    }

//----------------------------------------------------------------------------

    /**
     * Return the content of a column as an object
     * 
     * @param colName the name of column to look up
     * 
     * @return the object in the column or null if the column was empty (SQL NULL)
     * 
     * @throws SQLException 
     */
    public Object _c(String colName) throws SQLException
    {
        CachedRowSet rs = getColumnRowSet(colName);
        Object o = rs.getObject(1);
        if (rs.wasNull()) return null;
        return o;
    }

//----------------------------------------------------------------------------

    /**
     * Return the content of a column as a string
     * 
     * @param colName the name of column to look up
     * 
     * @return the string in the column or null if the column was empty (SQL NULL)
     * 
     * @throws SQLException 
     */
    public String c(String colName) throws SQLException
    {
        CachedRowSet rs = getColumnRowSet(colName);
        String o = rs.getString(1);
        if (rs.wasNull()) return null;
        return o;
    }

//----------------------------------------------------------------------------

    /**
     * Return the content of a column as an integer
     * 
     * @param colName the name of column to look up
     * 
     * @return the integer in the column or null if the column was empty (SQL NULL)
     * 
     * @throws SQLException 
     */
    public Integer asInt(String colName) throws SQLException
    {
        CachedRowSet rs = getColumnRowSet(colName);
        Integer o = rs.getInt(1);
        if (rs.wasNull()) return null;
        return o;
    }

//----------------------------------------------------------------------------

    /**
     * Return the content of a column as a double
     * 
     * @param colName the name of column to look up
     * 
     * @return the double in the column or null if the column was empty (SQL NULL)
     * 
     * @throws SQLException 
     */
    public Double asDouble(String colName) throws SQLException
    {
        CachedRowSet rs = getColumnRowSet(colName);
        Double o = rs.getDouble(1);
        if (rs.wasNull()) return null;
        return o;
    }

//----------------------------------------------------------------------------

    /**
     * Return the content of a column as a Date
     * 
     * @param colName the name of column to look up
     * 
     * @return the Date in the column or null if the column was empty (SQL NULL)
     * 
     * @throws SQLException 
     */
    public Date asDate(String colName) throws SQLException
    {
        CachedRowSet rs = getColumnRowSet(colName);
        Date o = rs.getTimestamp(1);
        if (rs.wasNull()) return null;
        return o;
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
