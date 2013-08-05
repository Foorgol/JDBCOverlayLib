/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;
import com.mysql.jdbc.CachedResultSetMetaData;
import com.sun.rowset.CachedRowSetImpl;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.rowset.CachedRowSet;

/**
 * Returns the results of an SQL query as ViewRows in an iterator
 * 
 * @author volker
 */
public class ViewRowIterator implements Iterable<ViewRow>
{
    /**
     * The CachedResultSet containing the query results
     */
    CachedRowSet rs;
    
    /**
     * A tag whether to finally call close() on the CachedResultSet or not
     */
    boolean doDispose;
    
   /**
     * Default constructor
     * 
     * @param _rs the not yet "next()" SQL result
     * @param _doDispose if true, the st will be finally disposed
     */
    public ViewRowIterator(CachedRowSet _rs, boolean _doDispose)
    {
        rs = _rs;
        doDispose = _doDispose;
    }
    
   /**
     * Constructor with doDispose implicitly set to "true"
     * 
     * @param _rs the not yet "next()" SQL result
     */
    public ViewRowIterator(CachedRowSet _rs)
    {
        this(_rs, true);
    }
    
    /**
     * The function returning the anonymous iterator class
     */
    @Override
    public Iterator<ViewRow> iterator()
    {
        // creating / instanciating the anonymous class
        return new Iterator<ViewRow> ()
        {
            private boolean hasStepped = false;
            private boolean hasData = false;
            
            /**
             * The hasNext() function of the anonymous class
             */
            @Override
            public boolean hasNext()
            {
                if (!hasStepped)
                {
                    try
                    {
                        hasData = rs.next();
                        hasStepped = true;
                        
                        // if there is no more data, dispose the statement
                        if ((doDispose) && (!hasData)) rs.close();
                    }
                    catch (Exception e)
                    {
                        System.err.println("ViewRowIterator: " + e.getMessage());
                        System.exit(42);
                    }
                }
                
                return hasData;
            }
            
            /**
             * The function returning the next item
             */
            @Override
            public ViewRow next()
            {
                if (!hasStepped) hasNext();
                if (!hasData) throw new java.util.NoSuchElementException();
                
                // reset the next flag
                hasStepped = false;
                
                try {
                    // create a copy result set of the current row
                    CachedRowSet thisRow = new CachedRowSetImpl();
                    thisRow.populate(rs.getOriginalRow());
                    
                    // create and return a new viewRow from this isolated row
                    return new ViewRow(thisRow);
                } catch (SQLException ex) {
                    Logger.getLogger(ViewRowIterator.class.getName()).log(Level.SEVERE, null, ex);
                    System.exit(42);
                }
                return null; // should be never reached
            }
            
            /**
             * A bogus, not implemented remove() function
             */
            @Override
            public void remove()
            {
                throw new UnsupportedOperationException("No remove() on ViewRowIterator!");
            }
        };
    }
}
