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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.commonlib5.utils.Pair;

/**
 * This class is used for doing SQL select statements on the database.
 * It should not be used for doing modifications via
 * update/delete/insert statements. If you would like to perform those functions,
 * please use a <a href="TableDataSet.html">TableDataSet</a>.
 *
 * <br>
 * Here is some example code for using a QueryDataSet.
 * <PRE>
 *  try(QueryDataSet qds = new QueryDataSet ( connection, "SELECT * from my_table" ))
 *  {
 *    qds.fetchRecords(10); // fetch the first 10 records
 *    for ( int i = 0; i &lt; qds.size(); i++ )
 *    {
 *      Record rec = qds.getRecord(i);
 *      int value = rec.getValue("column").asInt();
 *      log.debug ( "The value is: " + value );
 *    }
 *  }
 * </PRE>
 * It is important to always remember to close() a QueryDataSet in order to free the allocated resources
 * (or use a try with resource like the example).
 * <br>
 *
 * @author
 * <a href="mailto:jon@latchkey.com">Jon S. Stevens</a>
 * @version $Revision: 564 $
 */
public class QueryDataSet
   extends DataSet
{
  /**
   * Costruttore per classi derivate.
   *
   * @exception SQLException
   * @exception DataSetException
   */
  public QueryDataSet()
     throws SQLException, DataSetException
  {
  }

  /**
   * Creates a new QueryDataSet based on a connection and a select string.
   *
   * @param conn
   * @param selectStmt
   *
   * @exception SQLException
   * @exception DataSetException
   */
  public QueryDataSet(Connection conn, String selectStmt)
     throws SQLException, DataSetException
  {
    this.conn = conn;
    selectString = new StringBuilder(selectStmt);

    boolean ok = false;
    try
    {
      stmt = conn.createStatement();
      resultSet = stmt.executeQuery(selectStmt);
      schema = new Schema();
      schema.populate(resultSet.getMetaData(), null, conn);
      ok = true;
    }
    finally
    {
      if(!ok)
      {
        try
        {
          close();
        }
        catch(Exception ignored)
        {
          // ignore as another exception is already thrown
        }
      }
    }
  }

  /**
   * Create a new QueryDataSet based on an existing resultSet.
   *
   * @param resultSet
   *
   * @exception SQLException
   * @exception DataSetException
   */
  public QueryDataSet(ResultSet resultSet)
     throws SQLException, DataSetException
  {
    this.resultSet = resultSet;
    this.conn = resultSet.getStatement().getConnection();
    selectString = new StringBuilder();
    schema = new Schema();
    schema.populate(resultSet.getMetaData(), null, conn);
  }

  /**
   * get the Select String that was used to create this QueryDataSet.
   *
   * @return a select string
   */
  @Override
  public String getSelectString()
  {
    return selectString == null ? "" : selectString.toString();
  }

  /**
   * Returns numberOfResults records in a QueryDataSet as a List
   * of Record objects. Starting at record start. Used for
   * functionality like util.LargeSelect.
   *
   * @param start The index from which to start retrieving
   * <code>Record</code> objects from the data set.
   * @param numberOfResults The number of results to return (or
   * <code> -1</code> for all results).
   * @return A <code>List</code> of <code>Record</code> objects.
   * @exception Exception
   */
  public List<Record> getSelectResults(int start, int numberOfResults)
     throws Exception
  {
    List<Record> results = null;

    if(numberOfResults < 0)
    {
      results = new ArrayList<>();
      fetchRecords();
    }
    else
    {
      results = new ArrayList<>(numberOfResults);
      fetchRecords(start, numberOfResults);
    }

    int startRecord = 0;

    // Offset the correct number of records
    if(start > 0 && numberOfResults <= 0)
    {
      startRecord = start;
    }

    // Return a List of Record objects.
    for(int i = startRecord; i < size(); i++)
    {
      Record rec = getRecord(i);
      results.add(rec);
    }

    return results;
  }

  public static Record fetchFirstRecord(Connection dbCon, String sSQL)
     throws Exception
  {
    try (QueryDataSet qs = new QueryDataSet(dbCon, sSQL))
    {
      qs.fetchRecords(1);

      if(qs.size() == 0)
        return null;

      return qs.getRecord(0);
    }
  }

  public static Pair<Schema, Record> fetchFirstRecordAndSchema(Connection dbCon, String sSQL)
     throws Exception
  {
    try (QueryDataSet qs = new QueryDataSet(dbCon, sSQL))
    {
      qs.fetchRecords(1);

      if(qs.size() == 0)
        return new Pair<>(qs.schema, null);

      return new Pair<>(qs.schema, qs.getRecord(0));
    }
  }

  public static List<Record> fetchAllRecords(Connection dbCon, String sSQL)
     throws Exception
  {
    try (QueryDataSet qs = new QueryDataSet(dbCon, sSQL))
    {
      return qs.fetchAllRecords();
    }
  }

  public static Pair<Schema, List<Record>> fetchAllRecordsAndSchema(Connection dbCon, String sSQL)
     throws Exception
  {
    try (QueryDataSet qs = new QueryDataSet(dbCon, sSQL))
    {
      return new Pair<>(qs.schema, qs.fetchAllRecords());
    }
  }

  public static List<Record> fetchAllRecords(ResultSet rs)
     throws Exception
  {
    try (QueryDataSet qs = new QueryDataSet(rs))
    {
      return qs.fetchAllRecords();
    }
  }

  public static Pair<Schema, List<Record>> fetchAllRecordsAndSchema(ResultSet rs)
     throws Exception
  {
    try (QueryDataSet qs = new QueryDataSet(rs))
    {
      return new Pair<>(qs.schema, qs.fetchAllRecords());
    }
  }
}
