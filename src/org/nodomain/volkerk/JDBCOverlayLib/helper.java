/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author volker
 */
public class helper {
    /**
     * Converts any number of objects to a concatenated string
     * 
     * @param obj any number of objects
     * @return the resulting string
     */
    public static String strCat(Object ... obj)
    {
        String txt = "";
        for (int i=0; i < obj.length; i++)
        {
            txt += obj[i].toString();
        }
        
        return txt;
    }
    
//----------------------------------------------------------------------------
    
    /**
     * Takes a list of objects (e. g. ints), converts them to strings and concatenates them comma-separated
     * 
     * @param vals List / Collection of objects
     * @return Comma-separated string of all objects
     */
    public static String commaSepStringFromList(List<?> vals)
    {
        String result = "";
        
        for (Object v : vals)
        {
            result += v.toString() + ", ";
        }
        
        // remove the last ", " if the string is non-empty
        if (result.length() != 0)
        {
            result = result.substring(0, result.length() - 2);
        }
        
        return result;
    }
    
//----------------------------------------------------------------------------

    /**
     * Takes columnname / value pairs and constructs a WHERE clause using
     * placeholders ("?") and proper handling of NULL.
     * 
     * @param args a list of column / value pairs
     * 
     * @return an object of arrays with the WHERE-clause at index 0 and the parameters for the placeholders at index 1+
     */
    public static Object[] prepWhereClause(Object ... args)
    {
        String whereClause = "";
        
        // check for an EVEN number of arguments
        if ((args.length % 2) != 0) throw new IllegalArgumentException(
                "Need an even number of arguments (column / value pairs)");
        
        // for the return value, create a list of the parameter objects
        // plus the WHERE-clause at index 0
        ArrayList<Object> result = new ArrayList<>();
        result.add("dummy for where clause");

        for (int i=0; i < args.length; i += 2)
        {
            whereClause += args[i].toString();
            if (args[i+1] == null) whereClause += " IS NULL";
            else
            {
                whereClause += " = ?";
                result.add(args[i+1]);
            }
            if (i != (args.length - 2)) whereClause += " AND ";
        }

        result.set(0, whereClause);
        
        return result.toArray();
    }

//----------------------------------------------------------------------------
  
    public static HashMap<String, Object> ConvertListToColValMap(Object ... args)
    {
        HashMap<String, Object> result = new HashMap<>();
        
        if ((args == null) || (args.length == 0)) return result;
        
        if ((args.length % 2) != 0)
        {
            throw new IllegalArgumentException("Odd number of objects for column- / value pairs!");
        }
        
        for (int i = 0; i != (args.length / 2); i++)
        {
            result.put(args[2*i].toString(), args[2*i + 1]);
        }

        return result;
    }
    
//----------------------------------------------------------------------------
    
    /**
     * Rounds a double to a fixed number of decimals
     * 
     * @param val the value to round
     * @param dec the number of decimals
     * @return the original value rounded, not truncated, to the number of decimals
     */
    public static double roundDecimals(double val, int dec)
    {
        if (dec < 0) throw new IllegalArgumentException();
        
        long fac = (long) Math.pow(10, dec);
        val *= fac;
        long tmp = Math.round(val);
        return (double) tmp/fac;
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
