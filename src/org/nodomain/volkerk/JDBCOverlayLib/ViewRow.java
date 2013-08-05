/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;

import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import javax.sql.rowset.CachedRowSet;

/**
 * Represents a row in a view.
 * 
 * Typically, view rows do not have an ID column, so they are treated as
 * pure copies (hash-like) of the view row
 * 
 * @author volker
 */
public class ViewRow {
    
    /**
     * The view's contents as CachedRowSet
     */
    CachedRowSet viewData;
	
//----------------------------------------------------------------------------
    
    /**
     * Constructor which takes the view row's data from an already existing map
     * 
     * @param rs the CachedRowSet containing the view data
     */
    public ViewRow(CachedRowSet rs) throws SQLException
    {
        if (rs == null)
        {
            throw new IllegalArgumentException("CachedRowSet for view constructor may not be null!");
        }
        
        if (!(rs.next()))
        {
            throw new IllegalArgumentException("CachedRowSet for view constructor is empty!");
        }
        viewData = rs;
    }
	
//----------------------------------------------------------------------------
    
    /**
     * Return the contents of a column as an integer or null
     * 
     * @param colName the name of the column to convert
     * @return the integer value of that column
     */
    public Integer asInt(String colName) throws SQLException
    {
        Integer result = viewData.getInt(colName);
        if (viewData.wasNull()) return null;
        return result;
    }
	
//----------------------------------------------------------------------------
	
    /**
     * Return the contents of a column as an double or null
     * 
     * @param colName the name of the column to convert
     * @return the double value of that column
     */
    public Double asDouble(String colName) throws SQLException
    {
        Double result = viewData.getDouble(colName);
        if (viewData.wasNull()) return null;
        return result;
    }
    
//----------------------------------------------------------------------------
    
    /**
     * Return the contents of a column as an double or null, rounded to a number of decimals
     * 
     * @param colName the name of the column to convert
     * @param decimals the number of decimals to round to
     * @return the double value of that column
     */
    public Double asDouble(String colName, int decimals) throws SQLException
    {
        Double result = viewData.getDouble(colName);
        if (viewData.wasNull()) return null;
        return helper.roundDecimals(result, decimals);
    }
	
//----------------------------------------------------------------------------
    
    /**
     * Return the contents of a column as an Date or null
     * 
     * @param colName the name of the column to convert
     * @return a new Date-instance for that column
     */
    public Date asDate(String colName) throws SQLException
    {
        Date result = viewData.getTimestamp(colName);
        if (viewData.wasNull()) return null;
        return result;
    }

//----------------------------------------------------------------------------
    
    /**
     * Returns the content of a column as string or null
     * 
     * @param colName the name of the column to retrieve
     * @return a string for that column
     */
    public String c(String colName) throws SQLException
    {
        Object obj = viewData.getObject(colName);
        if (viewData.wasNull()) return null;
        return obj.toString();
    }
	
//----------------------------------------------------------------------------
    
    public boolean isEmpty(String colName) throws SQLException
    {
        Object obj = viewData.getObject(colName);
        if (viewData.wasNull()) return true;
        return false;
    }
	
//----------------------------------------------------------------------------
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
	
//----------------------------------------------------------------------------
    
}
