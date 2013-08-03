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
    @Override
    protected void populateTables() throws SQLException {
        // create a dummy table
        tableCreationHelper("t1", "i INTEGER", "f DOUBLE", "s VARCHAR(40)", "d DATETIME");
    }

    @Override
    protected void populateViews() {
    }
    
}
