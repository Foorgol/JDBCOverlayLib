/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import javax.sql.rowset.CachedRowSet;

/**
 *
 * @author volker
 */
public class JDBC_Tab extends JDBC_CommonTabularClass {
    /**
     * Default constructor; simply calls the parent constructor
     * 
     * @param _db the database object of the table
     * @param _tabName the name of the table
     */
    public JDBC_Tab(JDBC_GenericDB _db, String _tabName)
    {
        super(_db, _tabName, false);
    }
    
//----------------------------------------------------------------------------

    /**
     * Inserts a new row into the table and fills the row with data
     * 
     * @param colVal a Hashmap with maps column names to column contents
     * @return the ID of the inserted row
     */
    public int insertRow(HashMap<String, Object> colVal)
    {
        String sql = "INSERT INTO " + tabName + " (";
        ArrayList<Object> values = new ArrayList<>();
        ArrayList<String> placeholder = new ArrayList<>();
        ArrayList<String> columns = new ArrayList<>();
        
        if ((colVal != null) && (colVal.size() != 0))
        {
            // manually iterate the hash to assure that columns and values are in sync
            for (String colName : colVal.keySet())
            {
                columns.add(colName);
                values.add(colVal.get(colName));
                placeholder.add("?");
            }
        }
        
        // complete the SQL statement and do the insert
        sql += helper.commaSepStringFromList(columns);
        sql += ") VALUES (" + helper.commaSepStringFromList(placeholder) + ");";
        try
        {
            db.execNonQuery(sql, values.toArray());
            
            // return the last inserted ID
            return db.getLastInsertRowId();
        }
        catch (Exception e)
        {
            db.genericExceptionHandler(e, "Insert new row: ");
        }
        
        // we should never reach this
        return -1;

    }

//----------------------------------------------------------------------------

    /**
     * Updates the contents of an existing row
     * 
     * @param id the row's ID
     * @param colVal a hashmap which contains pair of column name and values to be set
     */
    public void updateRow(int id, HashMap<String, Object> colVal)
    {
        if ((colVal == null) || (colVal.isEmpty())) return;  // no update necessary
        
        String sql = "UPDATE " + tabName + " SET ";
        
        // a list of values for later insertion
        // into the sql statement
        ArrayList<Object> values = new ArrayList<>();
        
        for (String colName : colVal.keySet())
        {
            if ("id".equals(colName))
            {
                throw new IllegalArgumentException("The ID of a table row may not be altered!");
            }
            
            // add the value string to the sql-statement
            sql += colName + " = ?, ";
            values.add(colVal.get(colName));
        }
        
        // remove the trailing ", "
        sql = sql.substring(0, sql.length() - 2);
        
        sql += " WHERE id=" + String.valueOf(id) + ";";
        
        try
        {
            db.execNonQuery(sql, values.toArray());
        }
        catch (Exception e)
        {
            db.genericExceptionHandler(e, "Update row: ");
        }
    }

//----------------------------------------------------------------------------

    /**
     * Updates the contents of an existing row
     * 
     * @param id the row's ID
     * @param args a list of arguments which contains alternating column names and values to be set
     */
    public void updateRow(int id, Object ... args)
    {
        updateRow(id, helper.ConvertListToColValMap(args));
    }
    
//----------------------------------------------------------------------------

    /**
     * Inserts a new row
     * 
     * @param args a list of arguments which contains alternating column names and values to be set
     */
    public int insertRow(Object ... args)
    {
        return insertRow(helper.ConvertListToColValMap(args));
    }

//----------------------------------------------------------------------------

    /**
     * Returns the first row of a table that matches a WHERE clause
     * 
     * @param whereClause the clause to match
     * @return the TabRow instance for that row or null
     */
    public TabRow getSingleRowByWhereClause(String whereClause, Object ... params)
    {
        try
        {
            ArrayList<Object> tmp = new ArrayList<>(Arrays.asList(params));
            tmp.add(0, whereClause);
            
            return new TabRow(db, tabName, tmp.toArray());
        }
        catch (Exception e)
        {
            return null;
        }
    }

//----------------------------------------------------------------------------

    /**
     * Returns the first row of a table that matches a WHERE clause
     * 
     * @param params column/value pairs to match
     * @return the TabRow instance for that row or null
     */
    public TabRow getSingleRowByColumnValue(Object ... params)
    {
        try
        {
            return new TabRow(db, tabName, params);
        }
        catch (Exception e)
        {
            return null;
        }
    }

//----------------------------------------------------------------------------

    /**
     * Deletes rows that match a certain where clause
     * 
     * @param whereClause is the where clause to be matched
     * 
     * @return the number of rows affected
     */
    public int deleteRowsByWhereClause(String whereClause, Object ... params) throws SQLException
    {
        String sql = "DELETE FROM " + tabName + " WHERE " + whereClause;
        return db.execNonQuery(sql, params);
    }
    
//----------------------------------------------------------------------------
    
    /**
     * Deletes rows that match certain values in certain columns
     * 
     * Multiple column/value pairs are concatenated using AND
     * 
     * @param args a list of Strings / Objects for column / value pairs
     * 
     * @return the number of rows affected
     */
    public int deleteRowsByColumnValue(Object ... args) throws SQLException
    {
        ArrayList<Object> whereStuff = new ArrayList<>(Arrays.asList(helper.prepWhereClause(args)));
        
        String clause = whereStuff.get(0).toString();
        whereStuff.remove(0);
        
        if ((whereStuff.size() == 1) && (whereStuff.get(0) == null))
        {
            return deleteRowsByWhereClause(clause);
        }
        
        Object[] params = whereStuff.toArray();
        return deleteRowsByWhereClause(clause, params);
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

}
