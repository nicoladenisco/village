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
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.commonlib5.utils.ArrayMap;

/**
 * A Record represents a row in the database. It contains a collection of <a href="Value.html">Values</A> which are the
 * individual
 * contents of each column in the row.
 *
 * @author <a href="mailto:jon@latchkey.com">Jon S. Stevens</a>
 * @version $Revision: 568 $
 */
public class Record
{
  /** an array of Value objects, this is 1 based */
  private Value[] values;

  /** a 1 To 1 relationship between Values and whether they are clean or not */
  private boolean[] isClean;

  /** the parent DataSet for this Record */
  private DataSet parentDataSet;

  /** number of columns in this Record */
  private int numberOfColumns;

  /** this is the state of this record */
  private int saveType = 0;

  /** a saved copy of the schema for this Record */
  private Schema schema;

  /** if saveWithInsert prefer saveWithInsertAndGetGeneratedKeys */
  private boolean preferInsertAndGetGeneratedKeys = false;

  /**
   * This isn't used and doesn't do anything.
   */
  public Record()
  {
    // don't do anything
  }

  /**
   * Creates a new Record and sets the parent dataset to the passed in value. This method also creates the Value objects
   * which
   * are associated with this Record.
   *
   * @param ds TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   * @throws SQLException TODO: DOCUMENT ME!
   */
  public Record(DataSet ds)
     throws DataSetException, SQLException
  {
    setParentDataSet(ds);
    initializeRecord();
    createValues(dataset().resultSet());
  }

  /**
   * This is a special case method for Record. This case is really only used when DataSet.addRecord() is called because
   * we may
   * not have an existing ResultSet so there will not be any values in the Value objects that are created. Passing null
   * to
   * createValues forces the Value object to be created, but no processing to be done within the Value object
   * constructor.
   *
   * <P>
   * This method is a package method only because it is really not useful outside of the package.
   * </p>
   *
   * @param ds the dataset
   * @param addRecord whether or not this method is being called from DataSet.addRecord()
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   * @throws SQLException TODO: DOCUMENT ME!
   */
  @SuppressWarnings("OverridableMethodCallInConstructor")
  public Record(DataSet ds, boolean addRecord)
     throws DataSetException, SQLException
  {
    setParentDataSet(ds);
    initializeRecord();
    createValues(null);
  }

  public Record(Record origin)
     throws DataSetException, SQLException
  {
    setParentDataSet(origin.dataset());
    initializeRecord();
    createValuesClone(origin);
  }

  /**
   * Performs initialization for this Record.
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  private void initializeRecord()
     throws DataSetException
  {
    this.schema = dataset().schema();
    this.numberOfColumns = schema.numberOfColumns();
    this.values = new Value[size() + 1];
    this.isClean = new boolean[size() + 1];
    setSaveType(Enums.UNKNOWN);

    for(int i = 1; i <= size(); i++)
    {
      markValueClean(i);
      this.values[i] = null;
    }
  }

  /**
   * Creates the value objects for this Record. It is 1 based
   *
   * @param rs TODO: DOCUMENT ME!
   *
   * @exception DataSetException
   * @exception SQLException
   */
  private void createValues(ResultSet rs)
     throws DataSetException, SQLException
  {
    for(int i = 1; i <= size(); i++)
    {
      final Column column = schema().column(i);
      Value val = new Value(rs, column, i, column.typeEnum());
      this.values[i] = val;
    }
  }

  /**
   * Creates the value objects for this Record. It is 1 based
   *
   * @param rs TODO: DOCUMENT ME!
   *
   * @exception DataSetException
   * @exception SQLException
   */
  private void createValuesClone(Record origin)
     throws SQLException
  {
    for(int i = 1; i <= size(); i++)
    {
      Value valOrigin = origin.values[i];
      Value val = new Value(valOrigin.columnNumber(), valOrigin.column(), valOrigin.type(), valOrigin.getValue());
      this.values[i] = val;
    }
  }

  /**
   * Saves the data in this Record to the database. Uses the parent dataset's connection.
   *
   * @return 1 if the save completed. 0 otherwise.
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   * @throws SQLException TODO: DOCUMENT ME!
   */
  public int save()
     throws DataSetException, SQLException
  {
    return save(dataset().connection());
  }

  /**
   * Saves the data in this Record to the database. Uses the connection passed into it.
   *
   * @param connection TODO: DOCUMENT ME!
   *
   * @return 1 if the save completed. 0 otherwise.
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   * @throws SQLException TODO: DOCUMENT ME!
   */
  public int save(Connection connection)
     throws DataSetException, SQLException
  {
    int returnValue = 0;

    if(dataset() instanceof QueryDataSet)
    {
      throw new DataSetException("You cannot save a QueryDataSet. Please use a TableDataSet instead.");
    }

    if(!needsToBeSaved())
    {
      return returnValue;
    }

    if(toBeSavedWithInsert())
    {
      if(preferInsertAndGetGeneratedKeys)
        returnValue = saveWithInsertAndGetGeneratedKeys(connection);
      else
        returnValue = saveWithInsert(connection);
    }
    else if(toBeSavedWithUpdate())
    {
      returnValue = saveWithUpdate(connection);
    }
    else if(toBeSavedWithDelete())
    {
      returnValue = saveWithDelete(connection);
    }
    else
    {
      // comportamento di default: se il record è modificato ma non è nuovo cerca di salvarlo con una update
      if(!recordIsClean() && getSaveType() == Enums.UNKNOWN)
      {
        markForUpdate();
        returnValue = saveWithUpdate(connection);
      }
    }

    return returnValue;
  }

  /**
   * Saves the data in this Record to the database with an DELETE statement
   *
   * @param connection TODO: DOCUMENT ME!
   *
   * @return SQL DELETE statement
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   * @throws SQLException TODO: DOCUMENT ME!
   */
  public int saveWithDelete(Connection connection)
     throws DataSetException, SQLException
  {
    try (PreparedStatement stmt = connection.prepareStatement(getSaveString()))
    {
      int ps = 1;

      for(int i = 1; i <= dataset().keydef().size(); i++)
      {
        Value val = getValue(dataset().keydef().getAttrib(i));

        val.setPreparedStatementValue(stmt, ps++);
      }

      int ret = stmt.executeUpdate();

      // note that the actual deletion of the Record objects
      // from the TDS is now in the save() method of the TDS
      // instead of here. This fixes a bug where multiple
      // records would not be deleted properly because they
      // were being removed from here and the Records Vector
      // was getting out of sync with reality. So, just
      // mark them as needing to be removed here.
      setSaveType(Enums.ZOMBIE);

      if(ret > 1)
      {
        throw new SQLException("There were " + ret + " rows deleted with this records key value.");
      }

      return ret;
    }
  }

  /**
   * Saves the data in this Record to the database with an UPDATE statement
   *
   * @param connection TODO: DOCUMENT ME!
   *
   * @return SQL UPDATE statement
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   * @throws SQLException TODO: DOCUMENT ME!
   */
  public int saveWithUpdate(Connection connection)
     throws DataSetException, SQLException
  {
    try (PreparedStatement stmt = connection.prepareStatement(getSaveString()))
    {
      int ps = 1;

      for(int i = 1; i <= size(); i++)
      {
        Value val = getValue(i);

        if(!valueIsClean(i) && !schema().column(i).readOnly())
        {
          val.setPreparedStatementValue(stmt, ps++);
        }
      }

      for(int i = 1; i <= dataset().keydef().size(); i++)
      {
        Value val = getValue(dataset().keydef().getAttrib(i));

        val.setPreparedStatementValue(stmt, ps++);
      }

      int ret = stmt.executeUpdate();

      if(((TableDataSet) dataset()).refreshOnSave())
      {
        refresh(dataset().connection());
      }
      else
      {
        // Marks all of the values clean since they have now been saved
        markRecordClean();
      }

      setSaveType(Enums.AFTERUPDATE);

      if(ret > 1)
      {
        throw new SQLException("There were " + ret + " rows updated with this records key value.");
      }

      return ret;
    }
  }

  /**
   * Saves the data in this Record to the database with an INSERT statement
   *
   * @param connection TODO: DOCUMENT ME!
   *
   * @return SQL INSERT statement
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   * @throws SQLException TODO: DOCUMENT ME!
   */
  public int saveWithInsert(Connection connection)
     throws DataSetException, SQLException
  {
    try (PreparedStatement stmt = connection.prepareStatement(getSaveString()))
    {
      int ps = 1;

      for(int i = 1; i <= size(); i++)
      {
        Value val = getValue(i);

        if(!valueIsClean(i) && !schema().column(i).readOnly())
        {
          val.setPreparedStatementValue(stmt, ps++);
        }
      }

      int ret = stmt.executeUpdate();

      if(((TableDataSet) dataset()).refreshOnSave())
      {
        refresh(dataset().connection());
      }
      else
      {
        // Marks all of the values clean since they have now been saved
        markRecordClean();
      }

      setSaveType(Enums.AFTERINSERT);

      if(ret > 1)
      {
        throw new SQLException("There were " + ret + " rows inserted with this records key value.");
      }

      return ret;
    }
  }

  /**
   * Saves the data in this Record to the database with an INSERT statement.
   * Dopo la insert vengono recuperate eventuali valori generati da sequenze.
   *
   * @param connection TODO: DOCUMENT ME!
   *
   * @return SQL INSERT statement
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   * @throws SQLException TODO: DOCUMENT ME!
   */
  public int saveWithInsertAndGetGeneratedKeys(Connection connection)
     throws DataSetException, SQLException
  {
    Column primary = null;

    try (PreparedStatement stmt = connection.prepareStatement(getSaveString(), Statement.RETURN_GENERATED_KEYS))
    {
      int ps = 1;

      for(int i = 1; i <= size(); i++)
      {
        Column column = schema().column(i);

        if(column.isPrimaryKey())
          if(primary == null)
            primary = column;
          else
            throw new DataSetException("This function can be used only if table have one primay key with autoincrement.");

        if(!valueIsClean(i) && !column.readOnly())
        {
          Value val = getValue(i);
          val.setPreparedStatementValue(stmt, ps++);
        }
      }

      int ret = stmt.executeUpdate();

      if(ret != 0 && primary != null)
      {
        try (ResultSet rs = stmt.getGeneratedKeys())
        {
          if(rs != null && rs.next())
          {
            long value = rs.getLong(1);
            setValue(primary.name(), value);
          }
        }
      }

      if(((TableDataSet) dataset()).refreshOnSave())
      {
        refresh(dataset().connection());
      }
      else
      {
        // Marks all of the values clean since they have now been saved
        markRecordClean();
      }

      setSaveType(Enums.AFTERINSERT);

      if(ret > 1)
      {
        throw new SQLException("There were " + ret + " rows inserted with this records key value.");
      }

      return ret;
    }
  }

  /**
   * Builds the SQL UPDATE statement for this Record
   *
   * @return SQL UPDATE statement
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public String getUpdateSaveString()
     throws DataSetException
  {
    KeyDef kd = dataset().keydef();

    if((kd == null) || (kd.size() == 0))
    {
      throw new DataSetException(
         "You must specify KeyDef attributes for this TableDataSet in order to create a Record for update.");
    }
    else if(recordIsClean())
    {
      throw new DataSetException("You must Record.setValue() on a column before doing an update.");
    }

    StringBuilder iss1 = new StringBuilder(256);
    StringBuilder iss2 = new StringBuilder(256);
    boolean comma = false;

    for(int i = 1; i <= size(); i++)
    {
      if(!valueIsClean(i) && !schema().column(i).readOnly())
      {
        if(!comma)
        {
          iss1.append(schema().column(i).name());
          iss1.append(" = ?");
          comma = true;
        }
        else
        {
          iss1.append(", ");
          iss1.append(schema().column(i).name());
          iss1.append(" = ?");
        }
      }
    }

    comma = false;

    for(int i = 1; i <= kd.size(); i++)
    {
      String attrib = kd.getAttrib(i);

      if(!valueIsClean(schema().index(attrib)))
      {
        throw new DataSetException("The value for column '" + attrib + "' is a key value and cannot be updated.");
      }

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

    return "UPDATE " + schema().tableName() + " SET " + iss1.toString() + " WHERE " + iss2.toString();
  }

  /**
   * Builds the SQL DELETE statement for this Record
   *
   * @return SQL DELETE statement
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public String getDeleteSaveString()
     throws DataSetException
  {
    KeyDef kd = dataset().keydef();

    if((kd == null) || (kd.size() == 0))
    {
      throw new DataSetException("You must specify KeyDef attributes for this TableDataSet in order to delete a Record.");
    }

    StringBuilder iss1 = new StringBuilder(256);

    boolean comma = false;

    for(int i = 1; i <= kd.size(); i++)
    {
      if(!comma)
      {
        iss1.append(kd.getAttrib(i));
        iss1.append(" = ?");
        comma = true;
      }
      else
      {
        iss1.append(" AND ");
        iss1.append(kd.getAttrib(i));
        iss1.append(" = ? ");
      }
    }

    return "DELETE FROM " + schema().tableName() + " WHERE " + iss1.toString();
  }

  /**
   * Builds the SQL INSERT statement for this Record
   *
   * @return SQL INSERT statement
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public String getInsertSaveString()
     throws DataSetException
  {
    StringBuilder iss1 = new StringBuilder(256);
    StringBuilder iss2 = new StringBuilder(256);

    boolean comma = false;

    for(int i = 1; i <= size(); i++)
    {
      if(!valueIsClean(i) && !schema().column(i).readOnly())
      {
        if(!comma)
        {
          iss1.append(schema().column(i).name());
          iss2.append("?");
          comma = true;
        }
        else
        {
          iss1.append(", ").append(schema().column(i).name());
          iss2.append(", ?");
        }
      }
    }

    return "INSERT INTO " + schema().tableName() + " ( " + iss1.toString() + " ) VALUES ( " + iss2.toString() + " )";
  }

  /**
   * Gets the appropriate SQL string for this record.
   *
   * @return SQL string
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public String getSaveString()
     throws DataSetException
  {
    if(toBeSavedWithInsert())
    {
      return getInsertSaveString();
    }
    else if(toBeSavedWithUpdate())
    {
      return getUpdateSaveString();
    }
    else if(toBeSavedWithDelete())
    {
      return getDeleteSaveString();
    }
    else
    {
      throw new DataSetException("Not able to return save string: " + this.saveType);
    }
  }

  /**
   * gets the value at index i
   *
   * @param i TODO: DOCUMENT ME!
   *
   * @return the Value object at index i
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Value getValue(int i)
     throws DataSetException
  {
    if(i == 0)
    {
      throw new DataSetException("Values are 1 based!");
    }
    else if(i > size())
    {
      throw new DataSetException("Only " + size() + " columns exist!");
    }
    else if(values[i] == null)
    {
      throw new DataSetException("No values for the requested column!");
    }

    return values[i];
  }

  /**
   * TODO: DOCUMENT ME!
   *
   * @param columnName TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Value getValue(String columnName)
     throws DataSetException
  {
    return getValue(schema().index(columnName));
  }

  /**
   * the number of columns in this object
   *
   * @return the number of columns in this object
   */
  public int size()
  {
    return numberOfColumns;
  }

  /**
   * whether or not this Record is to be saved with an SQL insert statement
   *
   * @return true if saved with insert
   */
  public boolean toBeSavedWithInsert()
  {
    return (this.saveType == Enums.INSERT);
  }

  /**
   * whether or not this Record is to be saved with an SQL update statement
   *
   * @return true if saved with update
   */
  public boolean toBeSavedWithUpdate()
  {
    return (this.saveType == Enums.UPDATE);
  }

  /**
   * whether or not this Record is to be saved with an SQL delete statement
   *
   * @return true if saved with delete
   */
  public boolean toBeSavedWithDelete()
  {
    return (this.saveType == Enums.DELETE);
  }

  /**
   * Marks all the values in this record as clean.
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public void markRecordClean()
     throws DataSetException
  {
    for(int i = 1; i <= size(); i++)
    {
      markValueClean(i);
    }
  }

  /**
   * Marks this record to be inserted when a save is executed.
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public void markForInsert()
     throws DataSetException
  {
    if(dataset() instanceof QueryDataSet)
    {
      throw new DataSetException("You cannot mark a record in a QueryDataSet for insert");
    }

    setSaveType(Enums.INSERT);
  }

  /**
   * Marks this record to be updated when a save is executed.
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public void markForUpdate()
     throws DataSetException
  {
    if(dataset() instanceof QueryDataSet)
    {
      throw new DataSetException("You cannot mark a record in a QueryDataSet for update");
    }

    setSaveType(Enums.UPDATE);
  }

  /**
   * Marks this record to be deleted when a save is executed.
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record markToBeDeleted()
     throws DataSetException
  {
    if(dataset() instanceof QueryDataSet)
    {
      throw new DataSetException("You cannot mark a record in a QueryDataSet for deletion");
    }

    setSaveType(Enums.DELETE);

    return this;
  }

  /**
   * Unmarks a record that has been marked for deletion.
   *
   * <P>
   * WARNING: You must reset the save type before trying to save this record again.
   * </p>
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   *
   * @see #markForUpdate()
   * @see #markForInsert()
   * @see #markToBeDeleted()
   */
  public Record unmarkToBeDeleted()
     throws DataSetException
  {
    if(this.saveType == Enums.ZOMBIE)
    {
      throw new DataSetException("This record has already been deleted!");
    }

    setSaveType(Enums.UNKNOWN);

    return this;
  }

  /**
   * marks a value at a given position as clean.
   *
   * @param pos TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public void markValueClean(int pos)
     throws DataSetException
  {
    if(pos == 0)
    {
      throw new DataSetException("Value position must be greater than 0.");
    }
    else if(pos > size())
    {
      throw new DataSetException("Value position is greater than number of values.");
    }

    this.isClean[pos] = true;
  }

  /**
   * marks a value with a given column name as clean.
   *
   * @param columnName TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public void markValueClean(String columnName)
     throws DataSetException
  {
    markValueClean(schema().index(columnName));
  }

  /**
   * marks a value at a given position as dirty.
   *
   * @param pos TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public void markValueDirty(int pos)
     throws DataSetException
  {
    if(pos == 0)
    {
      throw new DataSetException("Value position must be greater than 0.");
    }
    else if(pos > size())
    {
      throw new DataSetException("Value position is greater than number of values.");
    }

    this.isClean[pos] = false;
  }

  /**
   * marks a value with a given column name as dirty.
   *
   * @param columnName TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public void markValueDirty(String columnName)
     throws DataSetException
  {
    markValueDirty(schema().index(columnName));
  }

  /**
   * sets the internal save type as one of the defined privates (ie: ZOMBIE)
   *
   * @param type TODO: DOCUMENT ME!
   */
  public void setSaveType(int type)
  {
    this.saveType = type;
  }

  /**
   * gets the internal save type as one of the defined privates (ie: ZOMBIE)
   *
   * @return TODO: DOCUMENT ME!
   */
  public int getSaveType()
  {
    return this.saveType;
  }

  public boolean isPreferInsertAndGetGeneratedKeys()
  {
    return preferInsertAndGetGeneratedKeys;
  }

  public void setPreferInsertAndGetGeneratedKeys(boolean preferInsertAndGetGeneratedKeys)
  {
    this.preferInsertAndGetGeneratedKeys = preferInsertAndGetGeneratedKeys;
  }

  /**
   * sets the value at pos with a BigDecimal
   *
   * @param pos TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(int pos, BigDecimal value)
     throws DataSetException
  {
    this.values[pos].setValue(value);
    markValueDirty(pos);

    return this;
  }

  /**
   * sets the value at pos with a boolean
   *
   * @param pos TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(int pos, boolean value)
     throws DataSetException
  {
    this.values[pos].setValue(value);
    markValueDirty(pos);

    return this;
  }

  /**
   * sets the value at pos with a byte[]
   *
   * @param pos TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(int pos, byte[] value)
     throws DataSetException
  {
    this.values[pos].setValue(value);
    markValueDirty(pos);

    return this;
  }

  /**
   * sets the value at pos with a java.util.Date
   *
   * @param pos TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(int pos, java.util.Date value)
     throws DataSetException
  {
    this.values[pos].setValue(value);
    markValueDirty(pos);

    return this;
  }

  /**
   * sets the value at pos with a java.sql.Date
   *
   * @param pos TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(int pos, java.sql.Date value)
     throws DataSetException
  {
    this.values[pos].setValue(value);
    markValueDirty(pos);

    return this;
  }

  /**
   * sets the value at pos with a double
   *
   * @param pos TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(int pos, double value)
     throws DataSetException
  {
    this.values[pos].setValue(value);
    markValueDirty(pos);

    return this;
  }

  /**
   * sets the value at pos with a float
   *
   * @param pos TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(int pos, float value)
     throws DataSetException
  {
    this.values[pos].setValue(value);
    markValueDirty(pos);

    return this;
  }

  /**
   * sets the value at pos with a int
   *
   * @param pos TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(int pos, int value)
     throws DataSetException
  {
    this.values[pos].setValue(value);
    markValueDirty(pos);

    return this;
  }

  /**
   * sets the value at pos with a long
   *
   * @param pos TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(int pos, long value)
     throws DataSetException
  {
    this.values[pos].setValue(value);
    markValueDirty(pos);

    return this;
  }

  /**
   * sets the value at pos with a String
   *
   * @param pos TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(int pos, String value)
     throws DataSetException
  {
    this.values[pos].setValue(value);
    markValueDirty(pos);

    return this;
  }

  /**
   * sets the value at pos with a java.sql.Time
   *
   * @param pos TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(int pos, java.sql.Time value)
     throws DataSetException
  {
    this.values[pos].setValue(value);
    markValueDirty(pos);

    return this;
  }

  /**
   * sets the value at pos with a java.sql.Timestamp
   *
   * @param pos TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(int pos, java.sql.Timestamp value)
     throws DataSetException
  {
    this.values[pos].setValue(value);
    markValueDirty(pos);

    return this;
  }

  /**
   * sets the value at pos with a Value
   *
   * @param pos TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(int pos, Value value)
     throws DataSetException
  {
    this.values[pos].setValue(value.getValue());
    markValueDirty(pos);

    return this;
  }

  /**
   * sets the value at column name with a BigDecimal
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(String columnName, BigDecimal value)
     throws DataSetException
  {
    setValue(schema().index(columnName), value);

    return this;
  }

  /**
   * sets the value at column name with a boolean
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(String columnName, boolean value)
     throws DataSetException
  {
    setValue(schema().index(columnName), value);

    return this;
  }

  /**
   * sets the value at column name with a byte[]
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(String columnName, byte[] value)
     throws DataSetException
  {
    setValue(schema().index(columnName), value);

    return this;
  }

  /**
   * sets the value at column name with a java.util.Date
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(String columnName, java.util.Date value)
     throws DataSetException
  {
    setValue(schema().index(columnName), value);

    return this;
  }

  /**
   * sets the value at column name with a java.sql.Date
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(String columnName, java.sql.Date value)
     throws DataSetException
  {
    setValue(schema().index(columnName), value);

    return this;
  }

  /**
   * sets the value at column name with a double
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(String columnName, double value)
     throws DataSetException
  {
    setValue(schema().index(columnName), value);

    return this;
  }

  /**
   * sets the value at column name with a float
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(String columnName, float value)
     throws DataSetException
  {
    setValue(schema().index(columnName), value);

    return this;
  }

  /**
   * sets the value at column name with a int
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(String columnName, int value)
     throws DataSetException
  {
    setValue(schema().index(columnName), value);

    return this;
  }

  /**
   * sets the value at column name with a long
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(String columnName, long value)
     throws DataSetException
  {
    setValue(schema().index(columnName), value);

    return this;
  }

  /**
   * sets the value at column name with a String
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(String columnName, String value)
     throws DataSetException
  {
    setValue(schema().index(columnName), value);

    return this;
  }

  /**
   * sets the value at column name with a java.sql.Time
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(String columnName, java.sql.Time value)
     throws DataSetException
  {
    setValue(schema().index(columnName), value);

    return this;
  }

  /**
   * sets the value at column name with a java.sql.Timestamp
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(String columnName, java.sql.Timestamp value)
     throws DataSetException
  {
    setValue(schema().index(columnName), value);

    return this;
  }

  /**
   * sets the value at column name with a Value
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValue(String columnName, Value value)
     throws DataSetException
  {
    setValue(schema().index(columnName), value);

    return this;
  }

  /**
   * sets the value at pos with a NULL
   *
   * @param pos TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValueNull(int pos)
     throws DataSetException
  {
    if(pos == 0)
    {
      throw new DataSetException("Value position must be greater than 0.");
    }
    else if(pos > size())
    {
      throw new DataSetException("Value position is greater than number of values.");
    }

    this.values[pos].setValue(null);
    markValueDirty(pos);

    return this;
  }

  /**
   * sets the value at column name with a NULL
   *
   * @param columnName TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Record setValueNull(String columnName)
     throws DataSetException
  {
    if((columnName == null) || (columnName.length() == 0))
    {
      throw new DataSetException("You must specify a column name!");
    }

    setValueNull(schema().index(columnName));

    return this;
  }

  /**
   * Determines if this record is a Zombie. A Zombie is a record that has been deleted from the database, but not yet
   * removed
   * from the DataSet.
   *
   * @return a boolean
   */
  public boolean isAZombie()
  {
    return (this.saveType == Enums.ZOMBIE);
  }

  /**
   * If the record is not clean, needs to be saved with an Update, Delete or Insert, it returns true.
   *
   * @return boolean
   */
  public boolean needsToBeSaved()
  {
    if(isAZombie())
      return false;

    if(toBeSavedWithDelete())
      return true;

    boolean clean = recordIsClean();

    if(clean)
      return false;

    return toBeSavedWithUpdate() || toBeSavedWithInsert();

    //return !isAZombie() || !recordIsClean() || toBeSavedWithUpdate() || toBeSavedWithDelete() || toBeSavedWithInsert();
  }

  /**
   * Determines whether or not a value stored in the record is clean.
   *
   * @param i TODO: DOCUMENT ME!
   *
   * @return true if clean
   */
  public boolean valueIsClean(int i)
  {
    return isClean[i];
  }

  /**
   * Determines whether or not a value stored in the record is clean.
   *
   * @param column TODO: DOCUMENT ME!
   *
   * @return true if clean
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public boolean valueIsClean(String column)
     throws DataSetException
  {
    return isClean[getValue(column).columnNumber()];
  }

  /**
   * Goes through all the values in the record to determine if it is clean or not.
   *
   * @return true if clean
   */
  public boolean recordIsClean()
  {
    for(int i = 1; i <= size(); i++)
    {
      if(!valueIsClean(i))
      {
        return false;
      }
    }

    return true;
  }

  /**
   * This method refreshes this Record's Value's. It can only be performed on a Record that has not been modified and
   * has been
   * created with a TableDataSet and corresponding KeyDef.
   *
   * @param connection
   *
   * @exception DataSetException
   * @exception SQLException
   */
  public void refresh(Connection connection)
     throws DataSetException, SQLException
  {
    if(toBeSavedWithDelete())
    {
      return;
    }
    else if(toBeSavedWithInsert())
    {
      throw new DataSetException("There is no way to refresh a record which has been created with addRecord().");
    }
    else if(dataset() instanceof QueryDataSet)
    {
      throw new DataSetException("You can only perform a refresh on Records created with a TableDataSet.");
    }

    try (PreparedStatement stmt = connection.prepareStatement(getRefreshQueryString()))
    {
      int ps = 1;
      for(int i = 1; i <= dataset().keydef().size(); i++)
      {
        Value val = getValue(dataset().keydef().getAttrib(i));

        if(val.isNull())
        {
          throw new DataSetException("You cannot execute an update with a null value for a KeyDef.");
        }

        val.setPreparedStatementValue(stmt, ps++);
      }

      try (ResultSet rs = stmt.executeQuery())
      {
        rs.next();
        initializeRecord();
        createValues(rs);
      }
    }
  }

  /**
   * This builds the SELECT statement in order to refresh the contents of this Record. It depends on a valid KeyDef to
   * exist and
   * it must have been created with a TableDataSet.
   *
   * @return the SELECT string
   *
   * @exception DataSetException
   */
  public String getRefreshQueryString()
     throws DataSetException
  {
    if((dataset().keydef() == null) || (dataset().keydef().size() == 0))
    {
      throw new DataSetException(
         "You can only perform a getRefreshQueryString on a TableDataSet that was created with a KeyDef.");
    }
    else if(!(dataset() instanceof TableDataSet))
    {
      throw new DataSetException("You can only perform a getRefreshQueryString on Records created with a TableDataSet.");
    }

    StringBuilder iss1 = new StringBuilder(256);
    StringBuilder iss2 = new StringBuilder(256);
    boolean comma = false;

    for(int i = 1; i <= size(); i++)
    {
      if(!comma)
      {
        iss1.append(schema().column(i).name());
        comma = true;
      }
      else
      {
        iss1.append(", ");
        iss1.append(schema().column(i).name());
      }
    }

    comma = false;

    for(int i = 1; i <= dataset().keydef().size(); i++)
    {
      String attrib = dataset().keydef().getAttrib(i);

      if(!valueIsClean(attrib))
      {
        throw new DataSetException("You cannot do a refresh from the database if the value "
           + "for a KeyDef column has been changed with a Record.setValue().");
      }

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

    return "SELECT " + iss1.toString() + " FROM " + schema().tableName() + " WHERE " + iss2.toString();
  }

  /**
   * TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public void saveWithoutStatusUpdate()
     throws DataSetException
  {
    throw new DataSetException("Record.saveWithoutStatusUpdate() is not yet implemented.");
  }

  /**
   * Gets the schema for the parent DataSet
   *
   * @return the schema for the parent DataSet
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public Schema schema()
     throws DataSetException
  {
    if(dataset() != null)
    {
      return this.schema;
    }
    else
    {
      throw new DataSetException("Internal Error: Record DataSet is null");
    }
  }

  /**
   * Gets the DataSet for this Record
   *
   * @return the DataSet for this Record
   */
  public DataSet dataset()
  {
    return this.parentDataSet;
  }

  /**
   * Sets the parent DataSet for this record.
   *
   * @param ds TODO: DOCUMENT ME!
   */
  public void setParentDataSet(DataSet ds)
  {
    this.parentDataSet = ds;
  }

  /**
   * return the value of each column as a string. Not yet implemented!
   *
   * @param valueseparator
   * @param maxwidths
   *
   * @return the formatted string
   *
   * @exception DataSetException
   */
  public String asFormattedString(String valueseparator, int[] maxwidths)
     throws DataSetException
  {
    throw new DataSetException("Not yet implemented!");
  }

  /**
   * This returns a representation of this Record
   *
   * @return java.lang.String
   */
  @Override
  public String toString()
  {
    try
    {
      StringBuilder sb = new StringBuilder(512);
      sb.append("{");

      for(int i = 1; i <= size(); i++)
      {
        sb.append("'").append(getValue(i).asString()).append("'");

        if(i < size())
        {
          sb.append(',');
        }
      }

      sb.append("}");
      return sb.toString();
    }
    catch(DataSetException e)
    {
      return "";
    }
  }

  public Record setValues(Map<String, Object> values)
     throws DataSetException
  {
    for(Map.Entry<String, Object> entry : values.entrySet())
    {
      String fname = entry.getKey();
      Object value = entry.getValue();

      int pos = schema().index(fname);
      this.values[pos].setValue(value);
      markValueDirty(pos);
    }

    return this;
  }

  /**
   * sets the value at column name with a BigDecimal
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public boolean setValueQuiet(String columnName, BigDecimal value)
     throws DataSetException
  {
    Column c = schema().findInSchemaIgnoreCaseQuiet(columnName);
    if(c == null)
      return false;

    setValue(schema().index(c.name()), value);
    return true;
  }

  /**
   * sets the value at column name with a boolean
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public boolean setValueQuiet(String columnName, boolean value)
     throws DataSetException
  {
    Column c = schema().findInSchemaIgnoreCaseQuiet(columnName);
    if(c == null)
      return false;

    setValue(schema().index(c.name()), value);
    return true;
  }

  /**
   * sets the value at column name with a byte[]
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public boolean setValueQuiet(String columnName, byte[] value)
     throws DataSetException
  {
    Column c = schema().findInSchemaIgnoreCaseQuiet(columnName);
    if(c == null)
      return false;

    setValue(schema().index(c.name()), value);
    return true;
  }

  /**
   * sets the value at column name with a java.util.Date
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public boolean setValueQuiet(String columnName, java.util.Date value)
     throws DataSetException
  {
    Column c = schema().findInSchemaIgnoreCaseQuiet(columnName);
    if(c == null)
      return false;

    setValue(schema().index(c.name()), value);
    return true;
  }

  /**
   * sets the value at column name with a java.sql.Date
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public boolean setValueQuiet(String columnName, java.sql.Date value)
     throws DataSetException
  {
    Column c = schema().findInSchemaIgnoreCaseQuiet(columnName);
    if(c == null)
      return false;

    setValue(schema().index(c.name()), value);
    return true;
  }

  /**
   * sets the value at column name with a double
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public boolean setValueQuiet(String columnName, double value)
     throws DataSetException
  {
    Column c = schema().findInSchemaIgnoreCaseQuiet(columnName);
    if(c == null)
      return false;

    setValue(schema().index(c.name()), value);
    return true;
  }

  /**
   * sets the value at column name with a float
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public boolean setValueQuiet(String columnName, float value)
     throws DataSetException
  {
    Column c = schema().findInSchemaIgnoreCaseQuiet(columnName);
    if(c == null)
      return false;

    setValue(schema().index(c.name()), value);
    return true;
  }

  /**
   * sets the value at column name with a int
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public boolean setValueQuiet(String columnName, int value)
     throws DataSetException
  {
    Column c = schema().findInSchemaIgnoreCaseQuiet(columnName);
    if(c == null)
      return false;

    setValue(schema().index(c.name()), value);
    return true;
  }

  /**
   * sets the value at column name with a long
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public boolean setValueQuiet(String columnName, long value)
     throws DataSetException
  {
    Column c = schema().findInSchemaIgnoreCaseQuiet(columnName);
    if(c == null)
      return false;

    setValue(schema().index(c.name()), value);
    return true;
  }

  /**
   * sets the value at column name with a String
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public boolean setValueQuiet(String columnName, String value)
     throws DataSetException
  {
    Column c = schema().findInSchemaIgnoreCaseQuiet(columnName);
    if(c == null)
      return false;

    setValue(schema().index(c.name()), value);
    return true;
  }

  /**
   * sets the value at column name with a java.sql.Time
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public boolean setValueQuiet(String columnName, java.sql.Time value)
     throws DataSetException
  {
    Column c = schema().findInSchemaIgnoreCaseQuiet(columnName);
    if(c == null)
      return false;

    setValue(schema().index(c.name()), value);
    return true;
  }

  /**
   * sets the value at column name with a java.sql.Timestamp
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public boolean setValueQuiet(String columnName, java.sql.Timestamp value)
     throws DataSetException
  {
    Column c = schema().findInSchemaIgnoreCaseQuiet(columnName);
    if(c == null)
      return false;

    setValue(schema().index(c.name()), value);
    return true;
  }

  /**
   * sets the value at column name with a Value
   *
   * @param columnName TODO: DOCUMENT ME!
   * @param value TODO: DOCUMENT ME!
   *
   * @return TODO: DOCUMENT ME!
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public boolean setValueQuiet(String columnName, Value value)
     throws DataSetException
  {
    Column c = schema().findInSchemaIgnoreCaseQuiet(columnName);
    if(c == null)
      return false;

    setValue(schema().index(c.name()), value);
    return true;
  }

  public Record setValuesQuiet(Map<String, Object> values)
     throws DataSetException
  {
    for(Map.Entry<String, Object> entry : values.entrySet())
    {
      String fname = entry.getKey();
      Object value = entry.getValue();
      Column c = schema().findInSchemaIgnoreCaseQuiet(fname);

      if(c != null)
      {
        int pos = schema().index(c.name());
        this.values[pos].setValue(value);
        markValueDirty(pos);
      }
    }

    return this;
  }

  public Map<Column, Value> getPrimaryKeyValues()
     throws DataSetException
  {
    List<Column> primaryKeys = schema().getPrimaryKeys();
    if(primaryKeys.isEmpty())
      return Collections.EMPTY_MAP;

    ArrayMap<Column, Value> rv = new ArrayMap<>();
    for(Column c : primaryKeys)
      rv.put(c, getValue(c.name()));

    return rv;
  }

  public Map<String, Object> getPrimaryKey()
     throws DataSetException
  {
    List<Column> primaryKeys = schema().getPrimaryKeys();
    if(primaryKeys.isEmpty())
      return Collections.EMPTY_MAP;

    ArrayMap<String, Object> rv = new ArrayMap<>();
    for(Column c : primaryKeys)
      rv.put(c.name(), getValue(c.name()).getValue());

    return rv;
  }
}
