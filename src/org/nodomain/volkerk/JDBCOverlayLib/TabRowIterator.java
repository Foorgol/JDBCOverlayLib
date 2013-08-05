/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;
import java.util.Iterator;
import javax.sql.rowset.CachedRowSet;

/**
 * Returns the results of an SQL query as TabRows in an iterator
 * 
 * @author volker
 */
public class TabRowIterator implements Iterable<TabRow>
{
     /**
     * the name of the associated table containing the row
     */
    protected String tabName;
    
    /**
     * the handle to the (parent) database
     */
    protected JDBC_GenericDB db;
    
    /**
     * The result set containing the query results
     */
    CachedRowSet rs;
    
    /**
     * A tag whether to finally call dispose() on the SQLiteStatement or not
     */
    boolean doDispose;
    
   /**
     * Default constructor
     * 
     * @param _db the associated database instance
     * @param _tabName the associated table name
     * @param _st the "prepare()" but not yet "step()" SQL query
     * @param _doDispose if true, the st will be finally disposed
     */
    public TabRowIterator(JDBC_GenericDB _db, String _tabName,
            CachedRowSet _rs, boolean _doDispose)
    {
        db = _db;
        tabName = _tabName;
        rs = _rs;
        doDispose = _doDispose;
    }
    
   /**
     * Constructor with doDispose implicitly set to "true"
     * 
     * @param _db the associated database instance
     * @param _tabName the associated table name
     * @param _st the "prepare()" but not yet "step()" SQL query
     */
    public TabRowIterator(JDBC_GenericDB _db, String _tabName, CachedRowSet _rs)
    {
        this(_db, _tabName, _rs, true);
    }
    
    /**
     * The function returning the anonymous iterator class
     */
    @Override
    public Iterator<TabRow> iterator()
    {
        // creating / instanciating the anonymous class
        return new Iterator<TabRow> ()
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
                        db.genericExceptionHandler(e, "TabRowIterator: ");
                    }
                }
                
                return hasData;
            }
            
            /**
             * The function returning the next item
             */
            @Override
            public TabRow next()
            {
                if (!hasStepped) hasNext();
                if (!hasData) throw new java.util.NoSuchElementException();
                
                // reset the next flag
                hasStepped = false;
                
                // instanciate and return the row
                int rowId = -1;
                try
                {
                    rowId = rs.getInt(1);
                }
                catch (Exception e)
                {
                    db.genericExceptionHandler(e, "TabRowIterator ID access: ");
                }
                
                return new TabRow(db, tabName, rowId);
            }
            
            /**
             * A bogus, not implemented remove() function
             */
            @Override
            public void remove()
            {
                throw new UnsupportedOperationException("No remove() on TabRowIterator!");
            }
        };
    }
}
