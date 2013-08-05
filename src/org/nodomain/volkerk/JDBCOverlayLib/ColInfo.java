/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;

/**
 * A small struct with schema information about a column
 *
 * @author volker
 */
public class ColInfo {

    protected int _cid;
    protected String _name;
    protected String _type;

    public ColInfo(int __id, String __name, String __type) {
        _cid = __id;
        _name = __name;
        _type = __type.toUpperCase();  // always capitalize
    }

    public int cid() {
        return _cid;
    }

    public String name() {
        return _name;
    }

    public String type() {
        return _type;
    }
}
