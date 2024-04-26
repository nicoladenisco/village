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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.commonlib5.lambda.ConsumerThrowException;

/**
 * This class is used for doing select/insert/delete/update on the database. A TableDataSet cannot be used to join
 * multiple tables
 * for an update, if you need join functionality on a select, you should use a
 * <a href="QueryDataSet.html">QueryDataSet</a>.
 *
 * <P>
 * Here is an example usage for this code that gets the first 10 records where column "a" = 1:
 * <PRE>
 *  KeyDef kd = new KeyDef().setAttrib("column");
 *  TableDataSet tds = new TableDataSet(connection, "table_name", kd );
 *  tds.where ("a=1" ); // WHERE a = 1
 *  tds.fetchRecords(10); // fetch first 10 records where column a=1
 *  for ( int i=0;i< tds.size(); i++ )
 *  {
 *  Record rec = tds.getRecord(i); // zero based
 *  String columnA = rec.getValue("a");
 *  if ( columnA.equals ("1") )
 *  System.out.print ("We got a column!");
 *  }
 *  tds.close();
 * </PRE>
 * </p>
 *
 * <P>
 * It is important to remember to always close() the TableDataSet when you are finished with it.
 * </p>
 *
 * <P>
 * As you can see, using a TableDataSet makes doing selects from the database trivial. You do not need to write any SQL
 * and it
 * makes it easy to cache a TableDataSet for future use within your application.
 * </p>
 *
 * @author <a href="mailto:jon@latchkey.com">Jon S. Stevens</a>
 * @version $Revision: 568 $
 */
public class TableDataSet
   extends DataSet
{
  /** the value for the sql where clause */
  private String where = null;

  /** the value for the sql order by clause */
  private String order = null;

  /** the value for the sql other clause */
  private String other = null;

  // by default this is false;
  /** TODO: DOCUMENT ME! */
  private boolean refreshOnSave = false;

  /**
   * Default constructor.
   *
   * @exception SQLException
   * @exception DataSetException
   */
  public TableDataSet()
     throws SQLException, DataSetException
  {
    super();
  }

  /**
   * Creates a new TableDataSet object.
   *
   * @param conn TODO: DOCUMENT ME!
   * @param tableName TODO: DOCUMENT ME!
   *
   * @throws SQLException TODO: DOCUMENT ME!
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public TableDataSet(Connection conn, String tableName)
     throws SQLException, DataSetException
  {
    super(conn, tableName);
  }

  /**
   * Creates a new TableDataSet object.
   *
   * @param conn TODO: DOCUMENT ME!
   * @param schema TODO: DOCUMENT ME!
   * @param keydef TODO: DOCUMENT ME!
   *
   * @throws SQLException TODO: DOCUMENT ME!
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public TableDataSet(Connection conn, Schema schema, KeyDef keydef)
     throws SQLException, DataSetException
  {
    super(conn, schema, keydef);
  }

  /**
   * Creates a new TableDataSet object.
   *
   * @param conn TODO: DOCUMENT ME!
   * @param tableName TODO: DOCUMENT ME!
   * @param keydef TODO: DOCUMENT ME!
   *
   * @throws SQLException TODO: DOCUMENT ME!
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public TableDataSet(Connection conn, String tableName, KeyDef keydef)
     throws SQLException, DataSetException
  {
    super(conn, tableName, keydef);
  }

  /**
   * Creates a new TableDataSet object.
   *
   * @param conn TODO: DOCUMENT ME!
   * @param schemaName
   * @param tableName TODO: DOCUMENT ME!
   * @param columns TODO: DOCUMENT ME!
   *
   * @throws SQLException TODO: DOCUMENT ME!
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public TableDataSet(Connection conn, String schemaName, String tableName, String columns)
     throws SQLException, DataSetException
  {
    super(conn, schemaName, tableName, columns);
  }

  /**
   * Creates a new TableDataSet object.
   *
   * @param conn TODO: DOCUMENT ME!
   * @param tableName TODO: DOCUMENT ME!
   * @param columns TODO: DOCUMENT ME!
   * @param keydef TODO: DOCUMENT ME!
   *
   * @throws SQLException TODO: DOCUMENT ME!
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public TableDataSet(Connection conn, String tableName, String columns, KeyDef keydef)
     throws SQLException, DataSetException
  {
    super(conn, tableName, columns, keydef);
  }

  /**
   * Fetch start to max records.start is at Record 0
   *
   * @param start
   * @param max
   * @param consumer
   *
   * @return an instance of myself
   *
   * @exception SQLException
   * @exception DataSetException
   */
  @Override
  public DataSet fetchRecords(int start, int max, ConsumerThrowException<Record> consumer)
     throws SQLException, DataSetException
  {
    buildSelectString();
    return super.fetchRecords(start, max, consumer);
  }

  @Override
  public void clear()
     throws DataSetException
  {
    super.clear();
    where = order = other = null;
    refreshOnSave = false;
  }

  /**
   * Saves all the records in the DataSet.
   *
   * @return total number of records updated/inserted/deleted
   *
   * @throws SQLException TODO: DOCUMENT ME!
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public int save()
     throws SQLException, DataSetException
  {
    return save(connection(), false);
  }

  /**
   * Saves all the records in the DataSet with the intransaction boolean value.
   *
   * @param intransaction TODO: DOCUMENT ME!
   *
   * @return total number of records updated/inserted/deleted
   *
   * @throws SQLException TODO: DOCUMENT ME!
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public int save(boolean intransaction)
     throws SQLException, DataSetException
  {
    return save(connection(), intransaction);
  }

  /**
   * Saves all the records in the DataSet with the given connection and intransaction boolean value.
   *
   * @param conn TODO: DOCUMENT ME!
   * @param intransaction TODO: DOCUMENT ME!
   *
   * @return total number of records updated/inserted/deleted
   *
   * @throws SQLException TODO: DOCUMENT ME!
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public int save(Connection conn, boolean intransaction)
     throws SQLException, DataSetException
  {
    int j = 0;

    for(Record rec : records)
    {
      rec.save(conn);
      j++;
    }

    // now go through and remove any records
    // that were previously marked as a zombie by the
    // delete process
    removeDeletedRecords();

    return j;
  }

  /**
   * Removes any records that are marked as a zombie.
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public void removeDeletedRecords()
     throws DataSetException
  {
    if(records == null)
      throw new DataSetException("Cache records is empty: use fetchRecord.");

    for(Record rec : new ArrayList<>(records))
    {
      if(rec.isAZombie())
      {
        removeRecord(rec);
      }
    }
  }

  /**
   * Sets the value for the SQL portion of the WHERE statement
   *
   * @param where TODO: DOCUMENT ME!
   *
   * @return instance of self
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public TableDataSet where(String where)
     throws DataSetException
  {
    if(where == null)
    {
      throw new DataSetException("null not allowed for where clause");
    }

    this.where = where;

    return this;
  }

  /**
   * Gets the value of the SQL portion of WHERE.
   *
   * @return string
   */
  public String getWhere()
  {
    return this.where;
  }

  /**
   * Sets the value for the SQL portion of the ORDER statement
   *
   * @param order TODO: DOCUMENT ME!
   *
   * @return instance of self
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public TableDataSet order(String order)
     throws DataSetException
  {
    if(order == null)
    {
      throw new DataSetException("null not allowed for order clause");
    }

    this.order = order;

    return this;
  }

  /**
   * Gets the value of the SQL portion of ORDER.
   *
   * @return string
   */
  public String getOrder()
  {
    return this.order;
  }

  /**
   * Sets the value for the SQL portion of the OTHER statement
   *
   * @param other TODO: DOCUMENT ME!
   *
   * @return instance of self
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public TableDataSet other(String other)
     throws DataSetException
  {
    if(other == null)
    {
      throw new DataSetException("null not allowed for other clause");
    }

    this.other = other;

    return this;
  }

  /**
   * Gets the value of the SQL portion of OTHER.
   *
   * @return string
   */
  public String getOther()
  {
    return this.other;
  }

  /**
   * This method refreshes all of the Records stored in this TableDataSet.
   *
   * @param conn TODO: DOCUMENT ME!
   *
   * @throws SQLException TODO: DOCUMENT ME!
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public void refresh(Connection conn)
     throws SQLException, DataSetException
  {
    if(records == null)
      throw new DataSetException("Cache records is empty: use fetchRecord.");

    for(Record rec : records)
    {
      rec.refresh(conn);
    }
  }

  /**
   * Setting this causes each Record to refresh itself when a save() is performed on it.
   *
   * <P>
   * Default value is false.
   * </p>
   *
   * @param val TODO: DOCUMENT ME!
   */
  public void setRefreshOnSave(boolean val)
  {
    this.refreshOnSave = val;
  }

  /**
   * Setting this causes each Record to refresh itself when a save() is performed on it.
   *
   * <P>
   * Default value is false.
   * </p>
   *
   * @return true if it is on; false otherwise
   */
  public boolean refreshOnSave()
  {
    return this.refreshOnSave;
  }

  /**
   * This sets additional SQL for the table name. The string appears after the table name. Sybase users would set this
   * to
   * "HOLDLOCK" to get repeatable reads.
   *
   * <P>
   * FIXME: Is this right? I don't use Sybase.
   * </p>
   *
   * @param tq TODO: DOCUMENT ME!
   *
   * @return an instance of self
   */
  public TableDataSet tableQualifier(String tq)
  {
    // go directly to schema() cause it is where tableName is stored
    schema().appendTableName(tq);

    return this;
  }

  /**
   * Builds the select string that was used to populate this TableDataSet.
   *
   * @return SQL select string
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  @Override
  public String getSelectString()
     throws DataSetException
  {
    buildSelectString();

    return this.selectString.toString();
  }

  /**
   * Used by getSelectString to build the select string that was used to populate this TableDataSet.
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  protected void buildSelectString()
     throws DataSetException
  {
    if(selectString == null)
    {
      selectString = new StringBuilder(256);
    }
    else
    {
      selectString.setLength(0);
    }

    selectString.append("SELECT ");
    selectString.append(schema.attributes());
    selectString.append(" FROM ");
    selectString.append(schema.tableName());

    if((this.where != null) && (this.where.length() > 0))
    {
      selectString.append(" WHERE ").append(this.where);
    }

    if((this.order != null) && (this.order.length() > 0))
    {
      selectString.append(" ORDER BY ").append(this.order);
    }

    if((this.other != null) && (this.other.length() > 0))
    {
      selectString.append(this.other);
    }
  }

  public DataSet fetchByGenericValues(Map<String, Object> keyValues)
     throws DataSetException
  {
    try
    {
      return fetchByGenericValues(keyValues, 0, ALL_RECORDS, null);
    }
    catch(DataSetException ex)
    {
      throw ex;
    }
    catch(Exception ex)
    {
      throw new DataSetException("Error fetching records.", ex);
    }
  }

  public DataSet fetchByGenericValues(Map<String, Object> values,
     int start, int max, ConsumerThrowException<Record> consumer)
     throws Exception
  {
    clear();
    ArrayList<String> lsChiavi = new ArrayList<>(values.keySet());
    lsChiavi.sort((a, b) -> a.compareTo(b));
    String sSQL = buildSelectStringWhere(lsChiavi);
    PreparedStatement lstm = conn.prepareStatement(sSQL);

    int ps = 1;
    for(String colName : lsChiavi)
    {
      Column col = schema.column(colName);
      Value val = new Value(ps, col, col.typeEnum(), values.get(colName));

      if(val.isNull())
        throw new DataSetException("Missing value for " + tableName() + "." + colName + ".");

      val.setPreparedStatementValue(lstm, ps++);
    }

    this.stmt = lstm;
    this.resultSet = lstm.executeQuery();
    populateRecords(start, max, consumer);
    return this;
  }

  public DataSet fetchByPrimaryKeys(Map<String, Object> keyValues)
     throws DataSetException
  {
    try
    {
      return fetchByPrimaryKeys(keyValues, 0, ALL_RECORDS, null);
    }
    catch(DataSetException ex)
    {
      throw ex;
    }
    catch(Exception ex)
    {
      throw new DataSetException("Error fetching records.", ex);
    }
  }

  public DataSet fetchByPrimaryKeys(Map<String, Object> keyValues,
     int start, int max, ConsumerThrowException<Record> consumer)
     throws Exception
  {
    clear();
    String sSQL = buildSelectStringKeydef();
    PreparedStatement lstm = conn.prepareStatement(sSQL);

    int ps = 1;
    for(int i = 1; i <= keydef().size(); i++)
    {
      String colName = keydef().getAttrib(i);
      Column col = schema.column(colName);
      Value val = new Value(i, col, col.typeEnum(), keyValues.get(colName));

      if(val.isNull())
        throw new DataSetException("Missing primary key value for " + tableName() + "." + colName + ".");

      val.setPreparedStatementValue(lstm, ps++);
    }

    this.stmt = lstm;
    this.resultSet = lstm.executeQuery();
    populateRecords(start, max, consumer);
    return this;
  }

  public DataSet fetchByPrimaryKeysValues(Map<Column, Value> keyValues)
     throws DataSetException
  {
    try
    {
      return fetchByPrimaryKeysValues(keyValues, 0, ALL_RECORDS, null);
    }
    catch(DataSetException ex)
    {
      throw ex;
    }
    catch(Exception ex)
    {
      throw new DataSetException("Error fetching records.", ex);
    }
  }

  public DataSet fetchByPrimaryKeysValues(Map<Column, Value> keyValues,
     int start, int max, ConsumerThrowException<Record> consumer)
     throws Exception
  {
    clear();
    String sSQL = buildSelectStringKeydef();
    PreparedStatement lstm = conn.prepareStatement(sSQL);

    int ps = 1;
    for(int i = 1; i <= keydef().size(); i++)
    {
      String colName = keydef().getAttrib(i);
      Column col = schema.column(colName);
      Value val = keyValues.get(col);

      if(val.isNull())
        throw new DataSetException("Missing primary key value for " + tableName() + "." + colName + ".");

      val.setPreparedStatementValue(lstm, ps++);
    }

    this.stmt = lstm;
    this.resultSet = lstm.executeQuery();
    populateRecords(start, max, consumer);
    return this;
  }

  protected String buildSelectStringKeydef()
     throws DataSetException
  {
    if(keydef() == null || keydef().isEmpty())
      throw new DataSetException("KeyDef not present in this dataset.");

    return buildSelectStringWhere(keydef().getAsList());
  }

  protected String buildSelectStringWhere(Collection<String> selectFields)
     throws DataSetException
  {
    StringBuilder iss1 = new StringBuilder(256);
    StringBuilder iss2 = new StringBuilder(256);
    boolean comma = false;

    for(int i = 1; i <= schema.numberOfColumns(); i++)
    {
      if(!comma)
      {
        iss1.append(schema.column(i).name());
        comma = true;
      }
      else
      {
        iss1.append(", ");
        iss1.append(schema.column(i).name());
      }
    }

    comma = false;

    for(String attrib : selectFields)
    {
      if(!comma)
      {
        iss2.append(attrib);
        iss2.append(" = ?");
        comma = true;
      }
      else
      {
        iss2.append(" AND ");
        iss2.append(attrib);
        iss2.append(" = ?");
      }
    }

    return "SELECT " + iss1.toString() + " FROM " + schema.tableName() + " WHERE " + iss2.toString();
  }

  public long getNextID()
     throws Exception
  {
    KeyDef keydef = keydef();

    if(keydef == null)
      throw new Exception("Missing KeyDef for this table.");

    if(keydef.size() != 1)
      throw new Exception("KeyDef must have one and only one column.");

    String campo = keydef.getAttrib(0);
    return VillageUtils.getMaxField(schema().tableName(), campo, null, conn) + 1;
  }

  public long getNextIDFromSequence(String sequenceName)
     throws Exception
  {
    KeyDef keydef = keydef();

    if(keydef == null)
      throw new Exception("Missing KeyDef for this table.");

    if(keydef.size() != 1)
      throw new Exception("KeyDef must have one and only one column.");

    return VillageUtils.getValueFromSequence(sequenceName, conn);
  }

  public Record fetchOneRecordOrNew(String where, boolean createIfNotExist)
     throws Exception
  {
    clear();
    where(where);
    fetchRecords(1);

    if(lastFetchSize() == 1)
      return getRecord(0);

    if(createIfNotExist)
      return addRecord();

    return null;
  }

  public static Record fetchOneRecord(String tableName, String where, Connection con)
     throws Exception
  {
    try (TableDataSet tds = new TableDataSet(con, tableName))
    {
      tds.where(where);
      tds.fetchRecords(1);
      return tds.lastFetchSize() == 1 ? tds.getRecord(0) : null;
    }
  }

  public static List<Record> fetchAllRecords(String tableName, String where, Connection con)
     throws Exception
  {
    try (TableDataSet tds = new TableDataSet(con, tableName))
    {
      tds.where(where);
      return tds.fetchAllRecords();
    }
  }
}
