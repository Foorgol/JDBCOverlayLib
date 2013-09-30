/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;

import java.sql.SQLException;

/**
 *
 * @author volker
 */
public class SampleDB extends JDBC_GenericDB {

    public SampleDB (DB_ENGINE t, String srv, int port, String name, String user, String pw) throws SQLException
    {
        super(t, srv, port, name, user, pw);
    }
    
    public SampleDB(String sqliteFileName, boolean createNew) throws SQLException {
        super(sqliteFileName, createNew);
    }
    
    @Override
    protected void populateTables() throws SQLException {
        // create a dummy table
        tableCreationHelper("t1", "i INTEGER", "f DOUBLE", "s VARCHAR(40)", "d DATETIME");
        tableCreationHelper("t2", "i INTEGER", "f DOUBLE", "s VARCHAR(40)", "d DATETIME");
    }

    @Override
    protected void populateViews() throws SQLException
    {
        viewCreationHelper("v1", "SELECT i,f,s FROM t1 WHERE i=84");
    }
    
}
