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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Nicola De Nisco
 */
public class TableDataSetTest
{
  public final DerbyTestHelper th = new DerbyTestHelper();

  public TableDataSetTest()
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
    try (TableDataSet tds = new TableDataSet(th.con, "stp.transcode"))
    {
      tds.fetchRecords();
      assertEquals(9, tds.size());

      Record r = tds.getRecord(0);
      assertNotNull(r);
      assertEquals(r.getValue(1).asString(), "a");
      assertEquals(r.getValue("app").asString(), "a");

      Schema s = tds.schema();
      assertEquals(4, s.numberOfColumns());
      assertEquals(3, s.getPrimaryKeys().size());
    }
  }

  /**
   * Test of removeDeletedRecords method, of class TableDataSet.
   */
  @Test
  public void testRemoveDeletedRecords()
     throws Exception
  {
    System.out.println("removeDeletedRecords");
  }

  /**
   * Test of refresh method, of class TableDataSet.
   */
  @Test
  public void testRefresh()
     throws Exception
  {
    System.out.println("refresh");
  }

  /**
   * Test of fetchByGenericValues method, of class TableDataSet.
   */
  @Test
  public void testFetchByGenericValues_Map()
     throws Exception
  {
    System.out.println("fetchByGenericValues");
    Map<String, Object> keyValues = new HashMap<>();
    keyValues.put("app", "a");

    try (TableDataSet tds = new TableDataSet(th.con, "stp.transcode"))
    {
      tds.fetchByGenericValues(keyValues);
      assertEquals(6, tds.size());

      Record r = tds.getRecord(0);
      assertNotNull(r);
      assertEquals(r.getValue(1).asString(), "a");
      assertEquals(r.getValue("app").asString(), "a");

      Schema s = tds.schema();
      assertEquals(4, s.numberOfColumns());
      assertEquals(3, s.getPrimaryKeys().size());
    }
  }

  /**
   * Test of fetchByPrimaryKeys method, of class TableDataSet.
   */
  @Test
  public void testFetchByPrimaryKeys_Map()
     throws Exception
  {
    System.out.println("fetchByPrimaryKeys");
    Map<String, Object> keyValues = new HashMap<>();
    keyValues.put("app", "a");

    try (TableDataSet tds = new TableDataSet(th.con, "stp.transcode"))
    {
      tds.fetchByGenericValues(keyValues);
      assertEquals(6, tds.size());

      Record r = tds.getRecord(0);
      assertNotNull(r);
      assertEquals(r.getValue(1).asString(), "a");
      assertEquals(r.getValue("app").asString(), "a");

      Schema s = tds.schema();
      assertEquals(4, s.numberOfColumns());
      assertEquals(3, s.getPrimaryKeys().size());
    }
  }

  /**
   * Test of getNextID method, of class TableDataSet.
   */
  @Test
  public void testGetNextID()
     throws Exception
  {
    System.out.println("getNextID");
  }

  /**
   * Test of getNextIDFromSequence method, of class TableDataSet.
   */
  @Test
  public void testGetNextIDFromSequence()
     throws Exception
  {
    System.out.println("getNextIDFromSequence");
  }

  /**
   * Test of fetchOneRecordOrNew method, of class TableDataSet.
   */
  @Test
  public void testFetchOneRecordOrNew()
     throws Exception
  {
    System.out.println("fetchOneRecordOrNew");
    try (TableDataSet tds = new TableDataSet(th.con, "stp.transcode"))
    {
      Record r1 = tds.fetchOneRecordOrNew("app='a'", true);
      assertEquals("a", r1.getValue("app").asOkString());

      Record r2 = tds.fetchOneRecordOrNew("app='d'", true);
      assertEquals(r2.getSaveType(), Enums.INSERT);

      r2.setValue("app", "d");
      r2.setValue("tipo", "d");
      r2.setValue("codice_vero", "PIPPO");
      r2.setValue("codice_app", "PLUTO");
      r2.save();

      Record r3 = tds.fetchOneRecordOrNew("app='d'", true);
      assertEquals("d", r3.getValue("app").asOkString());

      r3.markToBeDeleted();
      tds.save();
      assertEquals(0, tds.size());
    }
  }

  /**
   * Test of fetchOneRecord method, of class TableDataSet.
   */
  @Test
  public void testFetchOneRecord()
     throws Exception
  {
    System.out.println("fetchOneRecord");
    Record result = TableDataSet.fetchOneRecord("stp.transcode", "app='a'", th.con);
    assertEquals("a", result.getValue("app").asOkString());
  }

  /**
   * Test of fetchAllRecords method, of class TableDataSet.
   */
  @Test
  public void testFetchAllRecords()
     throws Exception
  {
    System.out.println("fetchAllRecords");
    List<Record> result = TableDataSet.fetchAllRecords("stp.transcode", "app='a'", th.con);
    assertEquals(6, result.size());
  }
}
