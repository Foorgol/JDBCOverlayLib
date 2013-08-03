/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nodomain.volkerk.JDBCOverlayLib;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import org.nodomain.volkerk.JDBCOverlayLib.helper.*;

/**
 *
 * @author volker
 */
abstract public class JDBC_GenericDB {
    
    protected static final int MYSQL_DEFAULT_PORT = 3306;
    
    /**
     * A logger for error messages
     */
    private final static Logger LOGGER = Logger.getLogger("JDBC Overlay");

    /**
     * The connection object for the database handled by this class
     */
    protected Connection conn;
    
    /**
     * The user name for the database server
     */
    protected String dbUser;
    
    /**
     * The password for the database server
     */
    protected String dbPasswd;
    
    /**
     * The name of the database to be accessed
     */
    protected String dbName;

    /**
     * An enum to identify the database type for the connection
     */
    public enum DB_ENGINE {
        SQLITE,
        MYSQL
    };
    
    /**
     * The database type for the connection
     */
    protected DB_ENGINE dbType;
    
    /**
     * The name of the server to connect to
     */
    protected String dbServer;
    
    /**
     * The server port to connect to
     */
    protected int dbPort;
    
    /**
     * A counter for executed queries; for debugging purposes only
     */
    protected long queryCounter = 0;

//----------------------------------------------------------------------------
    
    /**
     * Creates a new database instance and connects to the DBMS
     * 
     * @param t the database engine type
     * @param srv the name of the database server or null for DBMS-specific default
     * @param port the port of the database server to connect to or 0 for DBMS-specific default
     * @param name the database name to open; the database must exist
     * @param user the user name for the database connection (null if not applicable)
     * @param pw the password for the database connection (null if not applicable)
     */
    public JDBC_GenericDB(DB_ENGINE t, String srv, int port, String name, String user, String pw) throws SQLException {
        conn = null;
        dbType = t;
        dbName = name;
        dbServer = srv;
        dbUser = user;
        dbPasswd = pw;
        dbPort = port;
        
        Properties connProps = new Properties();
        
        // build the connection string
        String connStr = "";
        if (t == DB_ENGINE.MYSQL)
        {
            connProps.put("user", dbUser);
            connProps.put("password", dbPasswd);
            
            // apply defaults
            if ((dbServer == null) || (dbServer.equals(""))) dbServer = "localhost";
            if (dbPort == 0) dbPort = MYSQL_DEFAULT_PORT;
            
            connStr = "jdbc:mysql://" + dbServer + ":" + dbPort + "/" + dbName;
        }
        else if (t == DB_ENGINE.SQLITE)
        {
            throw new NotImplementedException();
        }
        
        // get the connection
        try
        {
            conn = DriverManager.getConnection(connStr, connProps);
        }
        catch (SQLException ex)
        {
            throw new IllegalArgumentException("Invalid connection parameters for database!");
        }
        
        // create tables and views
        populateTables();
        populateViews();
    }
    
//----------------------------------------------------------------------------

    /**
     * Close the database connection and free all resources
     */
    public void close()
    {
        if (conn != null)
        {
            try
            {
                conn.close();
            }
            catch (SQLException ex)
            {
                log(Level.SEVERE, null, ex);
            }
            conn = null;
        }
    }

//----------------------------------------------------------------------------

    /**
     * To be overridden by child classes for the actual table creation upon object initialization
     */
    abstract protected void populateTables() throws SQLException;

//----------------------------------------------------------------------------
    
    /**
     * To be overridden by child classes for the actual view creation upon object initialization
     */
    abstract protected void populateViews() throws SQLException;
    
//----------------------------------------------------------------------------
    
    /**
     * Executes a SQL-Query which returns no data
     * 
     * @param baseSqlStmt the SQL statement with placeholders ("?")
     * @param params the objects to fill the placeholders
     * 
     * @throws SQLException 
     */
    public void execNonQuery(String baseSqlStmt, Object ... params) throws SQLException
    {
        try (PreparedStatement st = prepStatement(baseSqlStmt, params))
        {
            st.executeUpdate();
        }
        catch (SQLException e)
        {
            log(Level.SEVERE, "ExecNonQuery failed: ", "\n",
                    "QUERY: ", baseSqlStmt, "\n",
                    "ERROR: ", e.getMessage());
            throw e;
        }
        
        queryCounter++;
    }
    
//----------------------------------------------------------------------------

    /**
     * Executes a SQL-Query which returns a complete ResultSet as data
     * 
     * @param baseSqlStmt the SQL statement with placeholders ("?")
     * @param params the objects to fill the placeholders
     * 
     * @throws SQLException
     * 
     * @return the ResultSet object for the retrieved data
     */
    public ResultSet execContentQuery(String baseSqlStmt, Object ... params) throws SQLException
    {
        ResultSet rs = null;
        
        try (PreparedStatement st = prepStatement(baseSqlStmt, params))
        {
            rs = st.executeQuery();
        }
        catch (SQLException e)
        {
            log(Level.SEVERE, "ExecContentQuery failed: ", "\n",
                    "QUERY: ", baseSqlStmt, "\n",
                    "ERROR: ", e.getMessage());
            throw e;
        }
        
        queryCounter++;
        
        return rs;
    }
    
//----------------------------------------------------------------------------

    /**
     * Executes a SQL-Query which returns a single value in the first column of the first row of the query result
     * 
     * @param baseSqlStmt the SQL statement with placeholders ("?")
     * @param params the objects to fill the placeholders
     * 
     * @throws SQLException 
     * 
     * @return the retrieved object or null
     */
    public Object execScalarQuery(String baseSqlStmt, Object ... params) throws SQLException
    {
        ResultSet rs = null;
        
        try (PreparedStatement st = prepStatement(baseSqlStmt, params))
        {
            rs = st.executeQuery();
            queryCounter++;
            
            if (!(rs.first()))
            {
                log("Scalar query returned no data!");
                return null;
            }
            
            return rs.getObject(1);
        }
        catch (SQLException e)
        {
            log(Level.SEVERE, "ExecContentQuery failed: ", "\n",
                    "QUERY: ", baseSqlStmt, "\n",
                    "ERROR: ", e.getMessage());
            throw e;
        }
    }
    
//----------------------------------------------------------------------------

    /**
     * Executes a SQL-Query which returns a single int in the first column of the first row of the query result
     * 
     * @param baseSqlStmt the SQL statement with placeholders ("?")
     * @param params the objects to fill the placeholders
     * 
     * @throws SQLException 
     * 
     * @return the retrieved Integer or null
     */
    public Object execScalarQueryInt(String baseSqlStmt, Object ... params) throws SQLException
    {
        Object o = execScalarQuery(baseSqlStmt, params);
        
        if (o != null)
        {
            return Integer.parseInt(o.toString());
        }
        
        return null;
    }
    
//----------------------------------------------------------------------------

    protected PreparedStatement prepStatement(String baseSqlStmt, Object ... params) throws SQLException
    {
        PreparedStatement result = conn.prepareStatement(baseSqlStmt);
        
        for (int i = 0; i < params.length; i++)
        {
            result.setObject(i+1, params[i]);
        }
        
        return result;
    }

//----------------------------------------------------------------------------
    
    /**
     * Logs a message with "INFO" level
     * @param msg arbitrary list of objects (e. g. strings) to log
     */
    protected void log(Object ... msg)
    {
        log(Level.INFO, helper.strCat(msg));
    }
    
//----------------------------------------------------------------------------
    
    /**
     * Logs a message with custom level
     * @param msg arbitrary list of objects (e. g. strings) to log
     */
    protected void log(Level lvl, Object ... msg)
    {
        LOGGER.log(lvl, helper.strCat(msg));
    }
    
//----------------------------------------------------------------------------
    
    /**
     * Helper function for easy table creation, e. g. from within populateTables
     * 
     * @param tabName contains the name of the table to be created
     * @param colDefs is a list of column definitions for this table
     */
    public void tableCreationHelper(String tabName, List<String> colDefs) throws SQLException
    {
        String sql = "CREATE TABLE IF NOT EXISTS " + tabName + " (";
        sql += "id INTEGER NOT NULL PRIMARY KEY ";
        
        if (dbType == DB_ENGINE.MYSQL) sql += "AUTO_INCREMENT";
        else  sql += "AUTOINCREMENT";
        
        sql += ", " + helper.commaSepStringFromList(colDefs);
        
        sql += ");";
        execNonQuery(sql);
    }
    
//----------------------------------------------------------------------------
    
    /**
     * Helper function for easy table creation, e. g. from within populateTables
     * 
     * @param tabName contains the name of the table to be created
     * @param colDefs is a list of column definitions for this table
     */
    public void tableCreationHelper(String tabName, String ... colDefs) throws SQLException
    {
        tableCreationHelper(tabName, Arrays.asList(colDefs));
    }
	
//----------------------------------------------------------------------------
    
    /**
     * Helper function for easy view creation, e. g. from within populateViews
     * 
     * @param viewName contains the name of the view to be created
     * @param selectStmt is the sql-select-statement for this view
     */    
    public void viewCreationHelper(String viewName, String selectStmt) throws SQLException
    {
        String sql = "CREATE VIEW IF NOT EXISTS";
        if (dbType == DB_ENGINE.MYSQL) sql = "CREATE OR REPLACE VIEW";
        
        sql += " " + viewName + " AS ";
        sql += selectStmt;
        execNonQuery(sql);
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
    
    
}
