/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;

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