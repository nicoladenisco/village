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
public class SchemaTest
{
  public final DerbyTestHelper th = new DerbyTestHelper();

  public SchemaTest()
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
}
