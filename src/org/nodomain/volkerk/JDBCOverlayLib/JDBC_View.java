/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;

/**
 *
 * @author volker
 */
public class JDBC_View extends JDBC_CommonTabularClass
{
    /**
     * Default constructor, which simply calls the constructor of the base class
     * 
     * @param _db the DB object this view is contained in
     * @param _viewName the name of the view
     */
    public JDBC_View (JDBC_GenericDB _db, String _viewName)
    {
        super(_db, _viewName, true);
    }
    
}
