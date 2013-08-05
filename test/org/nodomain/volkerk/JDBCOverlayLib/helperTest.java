/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;

import java.sql.SQLException;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author volker
 */
public class helperTest {
    
//----------------------------------------------------------------------------
    
    @Test
    public void testPrepWhereClause() throws SQLException
    {
        Object[] r;
        
        r = helper.prepWhereClause("c1", 42, "c2", null, "c3", "666");
        assert(r.length == 3);
        assertTrue(r[0].toString().equals("c1 = ? AND c2 IS NULL AND c3 = ?"));
        assertTrue(r[1] == 42);
        assertTrue(r[2] == "666");
        
        // odd number of args
        try
        {
            helper.prepWhereClause(1,2,3);
            fail("prepWhereClause accepted odd number of arguments!");
        }
        catch (Exception e) {}
    }
	
}