/*
 * Copyright (C) 2024 Nicola De Nisco
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.workingdogs.village;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import org.commonlib5.utils.Pair;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Nicola De Nisco
 */
public class QueryDataSetTest
{
  public final DerbyTestHelper th = new DerbyTestHelper();

  public QueryDataSetTest()
  {
  }

  @BeforeClass
  public static void setUpClass()
  {
  }

  @AfterClass
  public static void tearDownClass()
  {
  }

  @Before
  public void setUp()
     throws Exception
  {
    th.init();
    if(!th.existTable("stp.transcode"))
      th.buildDb1();
  }

  @After
  public void tearDown()
     throws Exception
  {
    th.shutdown();
  }

  @Test
  public void test()
     throws Exception
  {
    System.out.println("TEST GENERALE");
    final String sSQL = "SELECT * FROM stp.transcode";
    try (QueryDataSet qds = new QueryDataSet(th.con, sSQL))
    {
      assertEquals(qds.getSelectString(), sSQL);
      qds.fetchRecords();
      assertEquals(9, qds.size());

      Record r = qds.getRecord(0);
      assertNotNull(r);
      assertEquals(r.getValue(1).asString(), "a");
      assertEquals(r.getValue("app").asString(), "a");
    }
  }

  /**
   * Test of fetchFirstRecord method, of class QueryDataSet.
   */
  @Test
  public void testFetchFirstRecord()
     throws Exception
  {
    System.out.println("fetchFirstRecord");
    final String sSQL = "SELECT * FROM stp.transcode";
    Record r = QueryDataSet.fetchFirstRecord(th.con, sSQL);
    assertNotNull(r);
    assertEquals(r.getValue(1).asString(), "a");
    assertEquals(r.getValue("app").asString(), "a");
  }

  /**
   * Test of fetchFirstRecordAndSchema method, of class QueryDataSet.
   */
  @Test
  public void testFetchFirstRecordAndSchema()
     throws Exception
  {
    System.out.println("fetchFirstRecordAndSchema");

    final String sSQL = "SELECT * FROM stp.transcode";
    Pair<Schema, Record> result = QueryDataSet.fetchFirstRecordAndSchema(th.con, sSQL);
    assertNotNull(result);

    Record r = result.second;
    assertNotNull(r);
    assertEquals(r.getValue(1).asString(), "a");
    assertEquals(r.getValue("app").asString(), "a");

    Schema s = result.first;
    assertNotNull(s);
  }

  /**
   * Test of fetchAllRecords method, of class QueryDataSet.
   */
  @Test
  public void testFetchAllRecords()
     throws Exception
  {
    System.out.println("fetchAllRecords");
    final String sSQL = "SELECT * FROM stp.transcode";

    List<Record> ls1 = QueryDataSet.fetchAllRecords(th.con, sSQL);
    assertEquals(9, ls1.size());

    try (Statement st = th.con.createStatement(); ResultSet rs = st.executeQuery(sSQL))
    {
      List<Record> result = QueryDataSet.fetchAllRecords(rs);
      assertEquals(9, result.size());
    }
  }

  /**
   * Test of fetchAllRecordsAndSchema method, of class QueryDataSet.
   */
  @Test
  public void testFetchAllRecordsAndSchema()
     throws Exception
  {
    System.out.println("fetchAllRecordsAndSchema");
    final String sSQL = "SELECT * FROM stp.transcode";

    Pair<Schema, List<Record>> ls1 = QueryDataSet.fetchAllRecordsAndSchema(th.con, sSQL);
    assertEquals(9, ls1.second.size());

    try (Statement st = th.con.createStatement(); ResultSet rs = st.executeQuery(sSQL))
    {
      Pair<Schema, List<Record>> result = QueryDataSet.fetchAllRecordsAndSchema(rs);
      assertEquals(9, result.second.size());
    }
  }
}
