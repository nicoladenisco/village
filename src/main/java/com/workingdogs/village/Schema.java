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
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.commonlib5.utils.Pair;
import org.commonlib5.utils.StringOper;

/**
 * The Schema object represents the <a href="Column.html">Columns</a> in a database table. It contains a collection of <a
 * href="Column.html">Column</a> objects.
 *
 * @author <a href="mailto:jon@latchkey.com">Jon S. Stevens</a>
 * @author John D. McNally
 * @author Nicola De Nisco
 */
public final class Schema
{
  /** TODO: DOCUMENT ME! */
  private String schemaName, tableName;

  /** TODO: DOCUMENT ME! */
  private String columnsAttribute;

  /** TODO: DOCUMENT ME! */
  private int numberOfColumns;

  /** TODO: DOCUMENT ME! */
  private Column[] columns;

  /** a map of column name to position of each columen (see index()) NOTE: it is case insensitive */
  private final Map<String, Integer> columnNumberByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

  /** a permanent cache of all built schemas */
  private static final HashMap<String, Schema> schemaCache = new HashMap<>(256);

  /**
   * This attribute is used to complement columns in the event that this schema represents more than one table. Its keys
   * are
   * String contains table names and its elements are HashMaps containing columns.
   */
  private final HashMap<String, Map<String, Column>> tableHash = new HashMap<>(256);

  /**
   * Una cache per mantenere una corrispondenza fra nome tabella e nome schema
   */
  private static final Map<String, String> cacheSchemaTable = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

  /** TODO: DOCUMENT ME! */
  private boolean singleTable = true;

  public static final String[] TABLES_FILTER = new String[]
  {
    "TABLE"
  };
  public static final String[] VIEWS_FILTER = new String[]
  {
    "VIEW"
  };
  public static final String[] TABLES_VIEWS_FILTER = new String[]
  {
    "TABLE",
    "VIEW"
  };

  /**
   * A blank Schema object
   */
  public Schema()
  {
    this.tableName = "";
    this.columnsAttribute = null;
    this.numberOfColumns = 0;
  }

  /**
   * Initialize all table schemas reachable from this connection
   *
   * @param conn a database connection
   * @throws SQLException if retrieving the database meta data is unsuccessful
   */
  public static void initSchemas(Connection conn)
     throws SQLException
  {
    DatabaseMetaData databaseMetaData = conn.getMetaData();
    String connURL = databaseMetaData.getURL();

    try(ResultSet rsTables = databaseMetaData.getTables(conn.getCatalog(), null, null, TABLES_FILTER))
    {
      while(rsTables.next())
      {
        String schemaName = StringOper.okStr(rsTables.getString("TABLE_SCHEM"));
        String tableName = StringOper.okStr(rsTables.getString("TABLE_NAME"));
        cacheSchemaTable.put(tableName, schemaName);

        try(ResultSet rsColumns = databaseMetaData.getColumns(conn.getCatalog(), schemaName, tableName, null))
        {
          Schema schema = new Schema();

          schema.setSchemaName(schemaName);
          schema.setTableName(tableName);
          schema.setAttributes("*");
          schema.singleTable = true;
          schema.populate(rsColumns, conn.getCatalog(), databaseMetaData);

          if(schema.numberOfColumns > 0)
          {
            String keyValue = makeKeyHash(connURL, schema.schemaName, schema.tableName);

            synchronized(schemaCache)
            {
              schemaCache.put(keyValue, schema);
            }
          }
        }
      }
    }
  }

  public static String makeKeyHash(String connURL, String schemaName, String tableName)
  {
    return connURL + "|" + schemaName + "|" + tableName;
  }

  /**
   * Creates a Schema with all columns
   *
   * @param conn
   * @param tableName
   *
   * @return an instance of myself
   *
   * @exception SQLException
   * @exception DataSetException
   */
  public static Schema schema(Connection conn, String tableName)
     throws SQLException, DataSetException
  {
    return schema(conn, "", tableName, "*");
  }

  /**
   * Creates a Schema with the named columns in the columnsAttribute
   *
   * @param conn
   * @param schemaName
   * @param tableName
   * @param columnsAttribute
   *
   * @return an instance of myself
   *
   * @exception SQLException
   * @exception DataSetException
   */
  public static Schema schema(Connection conn, String schemaName, String tableName, String columnsAttribute)
     throws SQLException, DataSetException
  {
    return schema(conn, VillageUtils.getCorrectSchema(schemaName, tableName), columnsAttribute);
  }

  public static Schema schema(Connection conn, Pair<String, String> ts, String columnsAttribute)
     throws SQLException, DataSetException
  {
    Schema tableSchema = null;
    DatabaseMetaData dbMeta = conn.getMetaData();
    String keyValue = makeKeyHash(dbMeta.getURL(), ts.first, ts.second);
    columnsAttribute = StringOper.okStr(columnsAttribute, "*");

    synchronized(schemaCache)
    {
      tableSchema = (Schema) schemaCache.get(keyValue);

      if(tableSchema == null)
      {
        String sql = buildSchemaQuery(columnsAttribute, ts);

        try(PreparedStatement stmt = conn.prepareStatement(sql))
        {
          if(stmt != null)
          {
            stmt.executeQuery();
            tableSchema = new Schema();
            tableSchema.setSchemaName(ts.first);
            tableSchema.setTableName(ts.second);
            tableSchema.setAttributes(columnsAttribute);
            tableSchema.populate(stmt.getMetaData(), ts.first, ts.second, conn);
            schemaCache.put(keyValue, tableSchema);
          }
          else
          {
            throw new DataSetException("Couldn't retrieve schema for " + ts.second);
          }
        }
      }
    }

    return tableSchema;
  }

  private static String buildSchemaQuery(String columnsAttribute, Pair<String, String> ts)
  {
    StringBuilder sql = new StringBuilder(128);
    sql.append("SELECT ");
    sql.append(columnsAttribute);
    sql.append(" FROM ");
    if(ts.first != null && !ts.first.isEmpty())
      sql.append(ts.first).append(".");
    sql.append(ts.second);
    sql.append(" WHERE 1 = -1");
    //System.out.println("Schema sql: " + sql);
    return sql.toString();
  }

  /**
   * Appends data to the tableName that this schema was first created with.
   *
   * <P>
   * </p>
   *
   * @param app String to append to tableName
   *
   * @see TableDataSet#tableQualifier(java.lang.String)
   */
  public void appendTableName(String app)
  {
    this.tableName = this.tableName + " " + app;
  }

  /**
   * List of columns to select from the table
   *
   * @return the list of columns to select from the table
   */
  public String attributes()
  {
    return this.columnsAttribute;
  }

  /**
   * Returns the requested Column object at index i
   *
   * @param i
   *
   * @return the requested column
   *
   * @exception DataSetException
   */
  public Column column(int i)
     throws DataSetException
  {
    if(i == 0)
    {
      throw new DataSetException("Columns are 1 based");
    }
    else if(i > numberOfColumns)
    {
      throw new DataSetException("There are only " + numberOfColumns() + " available!");
    }

    try
    {
      return columns[i];
    }
    catch(Exception e)
    {
      throw new DataSetException("Column number: " + numberOfColumns() + " does not exist!");
    }
  }

  /**
   * Returns the requested Column object by name
   *
   * @param colName
   *
   * @return the requested column
   *
   * @exception DataSetException
   */
  public Column column(String colName)
     throws DataSetException
  {
    return column(index(colName));
  }

  /**
   * Returns the requested Column object by name
   *
   * @param colName
   *
   * @return the requested column
   *
   * @exception DataSetException
   */
  public Column getColumn(String colName)
     throws DataSetException
  {
    int dot = colName.indexOf('.');

    if(dot > 0)
    {
      String table = colName.substring(0, dot);
      String col = colName.substring(dot + 1);

      return getColumn(table, col);
    }

    return column(index(colName));
  }

  /**
   * Returns the requested Column object belonging to the specified table by name
   *
   * @param tableName
   * @param colName
   *
   * @return the requested column, null if a column by the specified name does not exist.
   *
   * @exception DataSetException
   */
  public Column getColumn(String tableName, String colName)
     throws DataSetException
  {
    Map<String, Column> ch = tableHash.get(tableName);
    return ch == null ? null : ch.get(colName);
  }

  /**
   * Returns an array of columns
   *
   * @return an array of columns
   */
  public Column[] getColumns()
  {
    return this.columns;
  }

  /**
   * returns the table name that this Schema represents
   *
   * @return the table name that this Schema represents
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public String getTableName()
     throws DataSetException
  {
    if(singleTable)
    {
      return tableName;
    }
    else
    {
      throw new DataSetException("This schema represents several tables.");
    }
  }

  /**
   * returns all table names that this Schema represents
   *
   * @return the table names that this Schema represents
   */
  public String[] getAllTableNames()
  {
    return (String[]) tableHash.keySet().toArray(new String[tableHash.size()]);
  }

  /**
   * Gets the index position of a named column. If multiple tables are represented and they have columns with the same
   * name,
   * this method returns the first one listed, if the table name is not specified.
   *
   * @param colName
   *
   * @return the requested column index integer
   *
   * @exception DataSetException
   */
  public int index(String colName)
     throws DataSetException
  {
    Integer position = columnNumberByName.get(colName);

    if(position != null)
    {
      return position;
    }
    else
    {
      throw new DataSetException("Column name: " + colName + " does not exist!");
    }
  }

  /**
   * Gets the index position of a named column.
   *
   * @param tableName
   * @param colName
   *
   * @return the requested column index integer
   *
   * @exception DataSetException
   */
  public int index(String tableName, String colName)
     throws DataSetException
  {
    return index(tableName + "." + colName);
  }

  /**
   * Checks to see if this DataSet represents one table in the database.
   *
   * @return true if only one table is represented, false otherwise.
   */
  public boolean isSingleTable()
  {
    return singleTable;
  }

  /**
   * Gets the number of columns in this Schema
   *
   * @return integer number of columns
   */
  public int numberOfColumns()
  {
    return this.numberOfColumns;
  }

  /**
   * Internal method which populates this Schema object with Columns.
   *
   * @param meta The meta data of the ResultSet used to build this Schema.
   * @param sname The name of the schema referenced in this schema, or null if unknown or multiple tables are
   * @param tname The name of the table referenced in this schema, or null if unknown or multiple tables are
   * involved.
   * @param con database connection
   *
   * @exception SQLException
   * @exception DataSetException
   */
  public void populate(ResultSetMetaData meta, String sname, String tname, Connection con)
     throws SQLException, DataSetException
  {
    numberOfColumns = meta.getColumnCount();
    columns = new Column[numberOfColumns() + 1];
    columnNumberByName.clear();

    DatabaseMetaData dbMeta = con.getMetaData();
    String connURL = dbMeta.getURL();
    LocalPrimaryCache lpc = new LocalPrimaryCache(con.getCatalog(), dbMeta);

    for(int i = 1; i <= numberOfColumns(); i++)
    {
      String metaSchemaName = getSecureSchemaName(meta, i, sname);
      String metaTableName = getSecureTableName(meta, i, tname);
      String metaColumnName = meta.getColumnName(i);

      if(tableName == null)
        setTableName(metaTableName);
      if(schemaName == null)
        setSchemaName(metaSchemaName);

      Column col = null;

      if(metaTableName.length() > 0 && connURL != null)
      {
        Schema tableSchema = null;
        String keyValue = makeKeyHash(connURL, metaSchemaName, metaTableName);

        synchronized(schemaCache)
        {
          tableSchema = (Schema) schemaCache.get(keyValue);
        }

        if(tableSchema != null)
        {
          try
          {
            col = tableSchema.column(metaColumnName);
          }
          catch(DataSetException e)
          {
            // column does not exist, ignore
          }
        }
      }

      // Not found in cache
      if(col == null)
      {
        col = new Column();

        int primaryInfo = tname == null ? 0 : lpc.findInPrimary(metaSchemaName, metaTableName, metaColumnName);
        col.populate(meta, i, metaTableName, metaColumnName, primaryInfo);
      }

      columns[i] = col;
      columnNumberByName.put(metaColumnName, i);
      columnNumberByName.put(metaTableName + "." + metaColumnName, i);

      if((i > 1) && !col.getTableName().equalsIgnoreCase(columns[i - 1].getTableName()))
      {
        singleTable = false;
      }
    }

    // Avoid creating a HashMap in the most common case where only one
    // table is involved, even though this makes the multiple table case
    // more expensive because the table/column info is duplicated.
    if(singleTable)
    {
      // If available, use a the caller supplied table name.
      if((tname != null) && (tname.length() > 0))
      {
        setTableName(tname);
      }
      else
      {
        // Since there's only one table involved, attempt to set the
        // table name to that of the first column.  Sybase jConnect
        // 5.2 and older will fail, in which case we are screwed.
        try
        {
          setTableName(columns[1].getTableName());
        }
        catch(Exception e)
        {
          setTableName("");
        }
      }
    }

    tableHash.clear();
    for(int i = 1; i <= numberOfColumns(); i++)
    {
      Map<String, Column> columnHash = tableHash.get(columns[i].getTableName());

      if(columnHash == null)
      {
        columnHash = new HashMap<>();
        tableHash.put(columns[i].getTableName(), columnHash);
      }

      columnHash.put(columns[i].name(), columns[i]);
    }
  }

  private String getSecureTableName(ResultSetMetaData meta, int i, String tableName)
  {
    String metaTableName;

    // Workaround for Sybase jConnect 5.2 and older.
    try
    {
      metaTableName = meta.getTableName(i);
      // ResultSetMetaData may report table name as the empty
      // string when a database-specific function has been
      // called to generate a Column.
      if((metaTableName == null) || metaTableName.equals(""))
      {
        if(tableName != null)
        {
          metaTableName = tableName;
        }
        else
        {
          metaTableName = "";
        }
      }
    }
    catch(Exception e)
    {
      if(tableName != null)
      {
        metaTableName = tableName;
      }
      else
      {
        metaTableName = "";
      }
    }

    return metaTableName;
  }

  private String getSecureSchemaName(ResultSetMetaData meta, int i, String schemaName)
  {
    try
    {
      return meta.getSchemaName(i);
    }
    catch(Exception e)
    {
      return schemaName;
    }
  }

  /**
   * Internal method which populates this Schema object with Columns.
   *
   * @param dbMeta The meta data of the database connection used to build this Schema.
   * @param catalog
   * @param databaseMetaData
   * @throws SQLException
   */
  public void populate(ResultSet dbMeta, String catalog, DatabaseMetaData databaseMetaData)
     throws SQLException
  {
    List cols = new ArrayList();
    columnNumberByName.clear();
    LocalPrimaryCache lpc = new LocalPrimaryCache(catalog, databaseMetaData);

    while(dbMeta.next())
    {
      Column c = new Column();

      c.populate(tableName,
         dbMeta.getString(4), // column name
         dbMeta.getString(6), // Data source dependent type name
         dbMeta.getInt(5), // SQL type from java.sql.Types
         dbMeta.getInt(11) == DatabaseMetaData.columnNullable, // is NULL allowed.
         lpc.findInPrimary(dbMeta.getString(2), tableName, dbMeta.getString(4)));

      cols.add(c);

      int position = dbMeta.getInt(17); // ordinal number
      columnNumberByName.put(c.name(), position);
      columnNumberByName.put(tableName + "." + c.name(), position);
    }

    if(!cols.isEmpty())
    {
      numberOfColumns = cols.size();
      columns = new Column[numberOfColumns() + 1];

      int i = 1;
      for(Iterator col = cols.iterator(); col.hasNext();)
      {
        columns[i++] = (Column) col.next();
      }
    }
  }

  /**
   * Sets the columns to select from the table
   *
   * @param attributes comma separated list of column names
   */
  public void setAttributes(String attributes)
  {
    this.columnsAttribute = attributes;
  }

  /**
   * Sets the table name that this Schema represents
   *
   * @param tableName
   */
  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  /**
   * returns the table name that this Schema represents
   *
   * @return the table name that this Schema represents
   *
   * @throws DataSetException TODO: DOCUMENT ME!
   */
  public String tableName()
     throws DataSetException
  {
    return getTableName();
  }

  public String getSchemaName()
  {
    return schemaName;
  }

  public void setSchemaName(String schemaName)
  {
    this.schemaName = schemaName;
  }

  public String getFullTableName()
     throws DataSetException
  {
    if(schemaName == null || schemaName.isEmpty())
    {
      return getTableName();
    }

    return schemaName + "." + getTableName();
  }

  /**
   * This returns a representation of this Schema
   *
   * @return a string
   */
  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder(512);
    sb.append('{');

    for(int i = 1; i <= numberOfColumns; i++)
    {
      sb.append('\'');

      if(!singleTable)
      {
        sb.append(columns[i].getTableName()).append('.');
      }

      sb.append(columns[i].name()).append('\'');

      if(i < numberOfColumns)
      {
        sb.append(',');
      }
    }

    sb.append('}');
    return sb.toString();
  }

  /**
   * Cerca la colonna in modo case insensitive.
   * La ricerca avviene su tutte le colonne, anche
   * se questo schema fa riferimento a più tabelle (query).
   * @param nomeTabella nome della tabella da cercare
   * @param nomeColonna nome della colonna da cercare
   * @return la colonna corrispondente oppure null
   * @throws DataSetException solo in caso di errore
   */
  public Column findInSchemaIgnoreCaseQuiet(String nomeTabella, String nomeColonna)
     throws DataSetException
  {
    Integer i = columnNumberByName.get(nomeTabella + "." + nomeColonna);
    return i == null ? null : columns[i];
  }

  /**
   * Cerca la colonna in modo case insensitive.
   * La ricerca avviene su tutte le colonne, anche
   * se questo schema fa riferimento a più tabelle (query).
   * @param nomeColonna nome della colonna da cercare
   * @return la colonna corrispondente oppure null
   * @throws DataSetException solo in caso di errore
   */
  public Column findInSchemaIgnoreCaseQuiet(String nomeColonna)
     throws DataSetException
  {
    Integer i = columnNumberByName.get(nomeColonna);
    return i == null ? null : columns[i];
  }

  /**
   * Cerca la colonna in modo case insensitive.
   * La ricerca avviene su tutte le colonne, anche
   * se questo schema fa riferimento a più tabelle (query).
   * @param nomeColonna nome della colonna da cercare
   * @return la colonna corrispondente
   * @throws DataSetException in caso di errore o colonna non trovata
   */
  public Column findInSchemaIgnoreCase(String nomeColonna)
     throws DataSetException
  {
    Column col = findInSchemaIgnoreCaseQuiet(nomeColonna);

    if(col != null)
      return col;

    if(singleTable)
      throw new DataSetException(String.format(
         "Field %s not found in table %s.",
         nomeColonna, tableName));

    throw new DataSetException(String.format(
       "Field %s not found.",
       nomeColonna));
  }

  /**
   * Return the primary keys in this schema.
   * @return primary keys
   */
  public List<Column> getPrimaryKeys()
  {
    ArrayList<Column> primaryKeys = new ArrayList<>();

    if(columns != null && columns.length > 1)
    {
      for(int i = 1; i < columns.length; i++)
      {
        Column col = columns[i];
        if(col.isPrimaryKey())
          primaryKeys.add(col);
      }

      if(primaryKeys.size() > 1)
        primaryKeys.sort((c1, c2) -> c1.getPrimaryIndex() - c2.getPrimaryIndex());
    }

    return primaryKeys;
  }
}
