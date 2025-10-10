package com.workingdogs.village;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.commonlib5.lambda.ConsumerThrowException;
import org.commonlib5.utils.Pair;

/**
 * The DataSet represents a table in the database. It is extended by <a href="QueryDataSet.html">QueryDataSet</a> and <a
 * href="TableDataSet.html">TableDataSet</a> and should not be used directly. A DataSet contains a <a
 * href="Schema.html">Schema</a> and potentially a collection of <a href="Record.html">Records</a>.
 *
 * @author <a href="mailto:jon@latchkey.com">Jon S. Stevens</a>
 * @version $Revision: 568 $
 */
public abstract class DataSet implements Closeable
{
  /** indicates that all records should be retrieved during a fetch */
  public static final int ALL_RECORDS = -1;

  /** this DataSet's schema object */
  protected Schema schema;

  /** this DataSet's collection of Record objects */
  protected List<Record> records = null;

  /** this DataSet's connection object */
  protected Connection conn;

  /** have all records been retrieved with the fetchRecords? */
  private boolean allRecordsRetrieved = false;

  /** if saveWithInsert prefer saveWithInsertAndGetGeneratedKeys */
  private boolean preferInsertAndGetGeneratedKeys = false;

  /** number of records retrieved */
  private int recordRetrievedCount = 0;

  /** number of records that were last fetched */
  private int lastFetchSize = 0;

  /** the columns in the SELECT statement for this DataSet */
  private String columns;

  /** the select string that was used to build this DataSet */
  protected StringBuilder selectString;

  /** the KeyDef for this DataSet */
  protected KeyDef keyDefValue;

  /** the result set for this DataSet */
  protected ResultSet resultSet;

  /** the Statement for this DataSet */
  protected Statement stmt;

  /** cache definizione chiavi primarie */
  protected static final HashMap<String, KeyDef> keydefCache = new HashMap<>(256);

  /**
   * Per classi derivate.
   *
   * @exception DataSetException
   * @exception SQLException
   */
  public DataSet()
     throws DataSetException, SQLException
  {
  }

  /**
   * Create a new DataSet with a connection and a Table name
   *
   * @param conn
   * @param tableName
   *
   * @exception DataSetException
   * @exception SQLException
   */
  public DataSet(Connection conn, String tableName)
     throws DataSetException, SQLException
  {
    this(conn, "", tableName);
  }

  /**
   * Create a new DataSet with a connection and a Table name
   *
   * @param conn
   * @param schemaName
   * @param tableName
   *
   * @exception DataSetException
   * @exception SQLException
   */
  public DataSet(Connection conn, String schemaName, String tableName)
     throws DataSetException, SQLException
  {
    this(conn, VillageUtils.getCorrectSchema(schemaName, tableName));
  }

  public DataSet(Connection conn, Pair<String, String> ts)
     throws DataSetException, SQLException
  {
    this.conn = conn;
    this.columns = "*";
    this.schema = Schema.schema(conn, ts, "*");

    // ricerca automatica della chiave primaria
    String keyValue = Schema.makeKeyHash(conn.getMetaData().getURL(), ts.first, ts.second);
    synchronized(keydefCache)
    {
      if((keyDefValue = keydefCache.get(keyValue)) == null)
      {
        keyDefValue = buildFromSchema();
        keydefCache.put(keyValue, keyDefValue);
      }
    }
  }

  /**
   * Create a new DataSet with a connection, schema and KeyDef
   *
   * @param conn
   * @param schema
   * @param keydef
   *
   * @exception DataSetException
   * @exception SQLException
   */
  public DataSet(Connection conn, Schema schema, KeyDef keydef)
     throws DataSetException, SQLException
  {
    if(conn == null)
    {
      throw new SQLException("Database connection could not be established!");
    }
    else if(schema == null)
    {
      throw new DataSetException("You need to specify a valid schema!");
    }
    else if(keydef == null)
    {
      throw new DataSetException("You need to specify a valid KeyDef!");
    }

    this.conn = conn;
    this.schema = schema;
    this.columns = "*";

    this.keyDefValue = keydef;
  }

  /**
   * Create a new DataSet with a connection, tablename and KeyDef
   *
   * @param conn
   * @param tableName
   * @param keydef
   *
   * @exception SQLException
   * @exception DataSetException
   */
  public DataSet(Connection conn, String tableName, KeyDef keydef)
     throws SQLException, DataSetException
  {
    this.conn = conn;
    this.keyDefValue = keydef;
    this.columns = "*";
    this.schema = Schema.schema(conn, tableName);
  }

  /**
   * Create a new DataSet with a connection, tablename and list of columns
   *
   * @param conn
   * @param schemaName
   * @param tableName
   * @param columns
   *
   * @exception SQLException
   * @exception DataSetException
   */
  public DataSet(Connection conn, String schemaName, String tableName, String columns)
     throws SQLException, DataSetException
  {
    this.conn = conn;
    this.columns = columns;
    this.schema = Schema.schema(conn, schemaName, tableName, columns);
  }

  /**
   * Create a new DataSet with a connection, tableName, columns and a KeyDef
   *
   * @param conn
   * @param tableName
   * @param columns
   * @param keyDef
   *
   * @exception SQLException
   * @exception DataSetException
   */
  public DataSet(Connection conn, String tableName, String columns, KeyDef keyDef)
     throws SQLException, DataSetException
  {
    this.conn = conn;
    this.columns = columns;
    this.keyDefValue = keyDef;
    this.schema = Schema.schema(conn, "", tableName, columns);
  }

  /**
   * Gets the ResultSet for this DataSet
   *
   * @return the result set for this DataSet
   *
   * @exception SQLException
   * @exception DataSetException
   */
  public ResultSet resultSet()
     throws SQLException, DataSetException
  {
    if(resultSet == null)
    {
      throw new DataSetException("ResultSet is null.");
    }

    return resultSet;
  }

  /**
   * Calls addRecord(DataSet)
   *
   * @return the added record
   *
   * @exception DataSetException
   * @exception SQLException
   */
  public Record addRecord()
     throws DataSetException, SQLException
  {
    return addRecord(this);
  }

  /**
   * Creates a new Record within this DataSet
   *
   * @param ds
   *
   * @return the added record
   *
   * @exception DataSetException
   * @exception SQLException
   */
  public Record addRecord(DataSet ds)
     throws DataSetException, SQLException
  {
    if(ds instanceof QueryDataSet)
    {
      throw new DataSetException("You cannot add records to a QueryDataSet.");
    }

    if(records == null)
    {
      records = new ArrayList<Record>();
    }

    Record rec = new Record(ds, true);
    rec.markForInsert();
    rec.setPreferInsertAndGetGeneratedKeys(ds.isPreferInsertAndGetGeneratedKeys());
    records.add(rec);

    return rec;
  }

  /**
   * Check if all the records have been retrieve
   *
   * @return true if all records have been retrieved
   */
  public boolean allRecordsRetrieved()
  {
    return this.allRecordsRetrieved;
  }

  /**
   * Set all records retrieved
   *
   * @param set TODO: DOCUMENT ME!
   */
  public void setAllRecordsRetrieved(boolean set)
  {
    this.allRecordsRetrieved = set;
  }

  /**
   * Remove a record from the DataSet's internal storage
   *
   * @param rec
   *
   * @return the record removed
   *
   * @exception DataSetException
   */
  public Record removeRecord(Record rec)
     throws DataSetException
  {
    Record removeRec = null;

    try
    {
      int loc = records.indexOf(rec);
      if(loc == -1)
        return null;

      removeRec = (Record) records.get(loc);
      records.remove(loc);
    }
    catch(Exception e)
    {
      throw new DataSetException("Record could not be removed!");
    }

    return removeRec;
  }

  /**
   * Remove all records from the DataSet and nulls those records out and close() the DataSet.
   *
   * @return an instance of myself
   */
  public DataSet clearRecords()
  {
    if(records != null)
    {
      records.clear();
      records = null;
    }

    recordRetrievedCount = 0;
    lastFetchSize = 0;

    return this;
  }

  /**
   * Removes the records from the DataSet, but does not null the records out
   *
   * @return an instance of myself
   */
  public DataSet releaseRecords()
  {
    if(records != null)
    {
      records.clear();
      records = null;
    }

    recordRetrievedCount = 0;
    lastFetchSize = 0;
    setAllRecordsRetrieved(false);

    return this;
  }

  /**
   * Releases the records, closes the ResultSet and the Statement,
   * and nulls the Schema and Connection references.
   * NOTE: connection is not closed in this method; the metod only set null the internal reference.
   *
   * @exception IOException
   */
  @Override
  public void close()
     throws IOException
  {
    releaseRecords();
    schema = null;

    Throwable sqlEx = null;

    try
    {
      if(resultSet != null)
      {
        resultSet.close();
      }
    }
    catch(SQLException e)
    {
      sqlEx = e;
    }

    resultSet = null;

    try
    {
      if(stmt != null)
      {
        stmt.close();
      }
    }
    catch(SQLException e)
    {
      sqlEx = e;
    }

    stmt = null;
    conn = null;

    if(sqlEx != null)
    {
      throw new IOException(sqlEx);
    }
  }

  /**
   * Pulizia selettiva.
   * Rilascia gli oggetti collegati alla connessione e
   * tutti i records caricati in memoria.
   * Dopo questo il DataSet è pronto per una nuova query.
   * @throws DataSetException
   */
  public void clear()
     throws DataSetException
  {
    selectString = null;
    releaseRecords();

    Throwable sqlEx = null;

    try
    {
      if(resultSet != null)
      {
        resultSet.close();
      }
    }
    catch(SQLException e)
    {
      sqlEx = e;
    }

    resultSet = null;

    try
    {
      if(stmt != null)
      {
        stmt.close();
      }
    }
    catch(SQLException e)
    {
      sqlEx = e;
    }

    stmt = null;

    if(sqlEx != null)
    {
      throw new DataSetException("", sqlEx);
    }
  }

  /**
   * Essentially the same as releaseRecords, but it won't work on a QueryDataSet that has been created with a ResultSet
   *
   * @return an instance of myself
   *
   * @exception DataSetException
   * @exception SQLException
   */
  public DataSet reset()
     throws DataSetException, SQLException
  {
    if(!((resultSet() != null) && (this instanceof QueryDataSet)))
    {
      return releaseRecords();
    }
    else
    {
      throw new DataSetException("You cannot call reset() on a QueryDataSet.");
    }
  }

  /**
   * Gets the current database connection
   *
   * @return a database connection
   *
   * @exception SQLException
   */
  public Connection connection()
     throws SQLException
  {
    return conn;
  }

  /**
   * Gets the Schema for this DataSet
   *
   * @return the Schema for this DataSet
   */
  public Schema schema()
  {
    return this.schema;
  }

  /**
   * Get Record at 0 based index position
   *
   * @param pos
   *
   * @return an instance of the found Record
   *
   * @exception DataSetException
   */
  public Record getRecord(int pos)
     throws DataSetException
  {
    if(containsRecord(pos))
    {
      Record rec = (Record) records.get(pos);

      if(this instanceof TableDataSet)
      {
        rec.markForUpdate();
      }

      recordRetrievedCount++;

      return rec;
    }

    throw new DataSetException("Record not found at index: " + pos);
  }

  /**
   * Find Record at 0 based index position. This is an internal alternative to getRecord which tries to be smart about
   * the type
   * of record it is.
   *
   * @param pos
   *
   * @return an instance of the found Record
   *
   * @exception DataSetException
   */
  public Record findRecord(int pos)
     throws DataSetException
  {
    if(containsRecord(pos))
    {
      return (Record) records.get(pos);
    }

    throw new DataSetException("Record not found at index: " + pos);
  }

  /**
   * Check to see if the DataSet contains a Record at 0 based position
   *
   * @param pos
   *
   * @return true if record exists
   */
  public boolean containsRecord(int pos)
  {
    try
    {
      if(records.get(pos) != null)
      {
        return true;
      }
    }
    catch(Exception e)
    {
      return false;
    }

    return false;
  }

  /**
   * Causes the DataSet to hit the database and fetch all the records.
   *
   * @return an instance of myself
   *
   * @exception SQLException
   * @exception DataSetException
   */
  final public DataSet fetchRecords()
     throws SQLException, DataSetException
  {
    return fetchRecords(ALL_RECORDS);
  }

  /**
   * Causes the DataSet to hit the database and fetch all the records.
   *
   * @param consumer an istance of records parser
   * @return an instance of myself
   *
   * @exception SQLException
   * @exception DataSetException
   */
  final public DataSet forEach(ConsumerThrowException<Record> consumer)
     throws SQLException, DataSetException
  {
    return fetchRecords(0, ALL_RECORDS, consumer);
  }

  /**
   * Causes the DataSet to hit the database and fetch max records.
   *
   * @param max
   *
   * @return an instance of myself
   *
   * @exception SQLException
   * @exception DataSetException
   */
  final public DataSet fetchRecords(int max)
     throws SQLException, DataSetException
  {
    return fetchRecords(0, max);
  }

  /**
   * Causes the DataSet to hit the database and fetch max records, starting at start.
   * Record count begins at 0.
   *
   * @param start
   * @param max
   *
   * @return an instance of myself
   *
   * @exception SQLException
   * @exception DataSetException
   */
  final public DataSet fetchRecords(int start, int max)
     throws SQLException, DataSetException
  {
    return fetchRecords(start, max, null);
  }

  /**
   * The number of records that were fetched with the last fetchRecords.
   *
   * @return int
   */
  public int lastFetchSize()
  {
    return lastFetchSize;
  }

  /**
   * gets the KeyDef object for this DataSet
   *
   * @return the keydef for this DataSet, this value can be null
   */
  public KeyDef keydef()
  {
    return this.keyDefValue;
  }

  /**
   * This returns a represention of this DataSet
   *
   * @return TODO: DOCUMENT ME!
   */
  @Override
  public String toString()
  {
    try
    {
      StringBuilder sb = new StringBuilder(512);

      if(schema != null)
      {
        sb.append(schema.toString());
      }

      // limita ai primi 10 record
      int limit = Math.min(10, size());
      for(int i = 0; i < limit; i++)
      {
        sb.append(findRecord(i).toString());
      }

      if(limit < size())
      {
        sb.append("... and other ").append(size() - limit).append(" records (").append(size()).append(" total)");
      }

      return sb.toString();
    }
    catch(DataSetException e)
    {
      return "{}";
    }
  }

  /**
   * Gets the tableName defined in the schema
   *
   * @return string
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public String tableName()
     throws DataSetException
  {
    return schema.tableName();
  }

  /**
   * Gets the tableName defined in the schema (schema.table).
   *
   * @return string
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public String getFullTableName()
     throws DataSetException
  {
    return schema.getFullTableName();
  }

  /**
   * Classes extending this class must implement this method.
   *
   * @return the select string
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public abstract String getSelectString()
     throws DataSetException;

  /**
   * Returns the columns attribute for the DataSet
   *
   * @return the columns attribute for the DataSet
   */
  public String getColumns()
  {
    return this.columns;
  }

  /**
   * Gets the number of Records in this DataSet. It is 0 based.
   *
   * @return number of Records in this DataSet
   */
  public int size()
  {
    if(records == null)
    {
      return 0;
    }

    return records.size();
  }

  /**
   * Ritorna tutti i record frutto della query.
   * La prima volta che viene chiamata esegue fetch e conversione dal resultset.
   * Se chiamata di nuovo restituisce solo l'array di record.
   * @return lista oggetti Record
   * @throws SQLException
   * @throws DataSetException
   */
  public List<Record> fetchAllRecords()
     throws SQLException, DataSetException
  {
    if(!allRecordsRetrieved())
      fetchRecords();

    return new ArrayList<>(records);
  }

  /**
   * Causes the DataSet to hit the database and fetch max records, starting at start.
   * Record count begins at 0.
   *
   * @param start
   * @param max
   * @param consumer my be null
   *
   * @return an instance of myself
   *
   * @exception SQLException
   * @exception DataSetException
   */
  public DataSet fetchRecords(int start, int max, ConsumerThrowException<Record> consumer)
     throws SQLException, DataSetException
  {
    if(max == 0)
    {
      throw new DataSetException("Max is 1 based and must be greater than 0!");
    }
    else if((lastFetchSize() > 0) && (records != null))
    {
      throw new DataSetException("You must call DataSet.clearRecords() before executing DataSet.fetchRecords() again!");
    }

    try
    {
      if((stmt == null) && (resultSet == null))
      {
        if(selectString == null)
        {
          selectString = new StringBuilder(256);
          selectString.append("SELECT ");
          selectString.append(schema.attributes());
          selectString.append(" FROM ");
          selectString.append(schema.getFullTableName());
        }

        stmt = connection().createStatement();
        resultSet = stmt.executeQuery(selectString.toString());
      }

      populateRecords(start, max, consumer);
    }
    catch(SQLException | DataSetException e)
    {
      throw e;
    }
    catch(Exception e)
    {
      throw new DataSetException(e.getMessage(), e);
    }
    finally
    {
      if(resultSet != null)
      {
        resultSet.close();
        resultSet = null;
      }

      if(stmt != null)
      {
        stmt.close();
        stmt = null;
      }
    }

    return this;
  }

  /**
   * Build a keydef from the schema.
   * @return a keydef or null if not primary keys found
   */
  public KeyDef buildFromSchema()
  {
    List<Column> primaryKeys = getPrimaryKeys();

    if(primaryKeys.isEmpty())
      return null;

    KeyDef rv = new KeyDef();
    primaryKeys.forEach((c) -> rv.addAttrib(c.name()));
    return rv;
  }

  /**
   * Return the primary keys in this schema.
   * @return primary keys
   */
  public List<Column> getPrimaryKeys()
  {
    return schema.getPrimaryKeys();
  }

  /**
   * Costruisce l'elenco dei record.
   * Dopo aver creato statement e resultset viene chiamata
   * questa funzione per leggere il resultset e creare l'elenco degli oggetti Record.
   * @param max
   * @param start
   * @param consumer
   * @throws Exception
   */
  protected void populateRecords(int start, int max, ConsumerThrowException<Record> consumer)
     throws Exception
  {
    if(resultSet != null)
    {
      if((records == null) && (max > 0))
      {
        records = new ArrayList<Record>(max);
      }
      else
      {
        records = new ArrayList<Record>();
      }

      int startCounter = 0;
      int fetchCount = 0;

      while(!allRecordsRetrieved())
      {
        if(fetchCount == max)
        {
          break;
        }

        if(resultSet.next())
        {
          if(startCounter >= start)
          {
            Record rec = new Record(this);
            records.add(rec);

            if(consumer != null)
              consumer.accept(rec);

            fetchCount++;
          }
          else
          {
            startCounter++;
          }
        }
        else
        {
          setAllRecordsRetrieved(true);
          break;
        }
      }

      lastFetchSize = fetchCount;
    }
  }

  public Connection getConnection()
  {
    return conn;
  }

  public void setConnection(Connection conn)
  {
    this.conn = conn;
  }

  public boolean isPreferInsertAndGetGeneratedKeys()
  {
    return preferInsertAndGetGeneratedKeys;
  }

  public void setPreferInsertAndGetGeneratedKeys(boolean preferInsertAndGetGeneratedKeys)
  {
    this.preferInsertAndGetGeneratedKeys = preferInsertAndGetGeneratedKeys;
  }
}
