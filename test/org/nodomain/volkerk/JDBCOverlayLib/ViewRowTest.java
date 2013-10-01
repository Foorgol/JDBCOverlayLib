/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;

import java.sql.SQLException;
import org.junit.Test;
import static org.junit.Assert.*;

import static org.nodomain.volkerk.JDBCOverlayLib.JDBC_GenericDB.DB_ENGINE;

/**
 *
 * @author volker
 */
public class ViewRowTest extends DatabaseTestScenario {
    
    @Test
    public void testColumnAccess() throws SQLException
    {
        _testColumnAccess(DB_ENGINE.SQLITE);
        _testColumnAccess(DB_ENGINE.MYSQL);
    }
    
    public void _testColumnAccess(DB_ENGINE engine) throws SQLException
    {
        SampleDB db = getScenario01(engine);
        JDBC_View v = db.v("v1");
        
        ViewRow r2 = null;
        ViewRow r3 = null;
        
        // get the second and the third row from the view
        int cnt = 0;
        for (ViewRow tmp : (ViewRowIterator) v.allRows())
        {
            cnt++;
            if (cnt == 2) r2=tmp;
            if (cnt == 3) r3=tmp;
        }
        assertTrue(cnt == 3);
        
        // check row 2
        assertTrue(r2.asInt("i") == 84);
        assertNull(r2.asDouble("f"));
        assertTrue(r2.c("s").equals("Hoi"));
        
        // check row 3
        assertTrue(r3.asInt("i") == 84);
        assertTrue(r3.asDouble("f") == 42.42);
        assertTrue(r3.c("s").equals("Ho"));
    }
    
}