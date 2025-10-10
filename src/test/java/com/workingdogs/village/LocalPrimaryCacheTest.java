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

import java.sql.DatabaseMetaData;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test per LocalPrimaryCache.
 *
 * @author Nicola De Nisco
 */
public class LocalPrimaryCacheTest
{
  public final DerbyTestHelper th = new DerbyTestHelper();

  public LocalPrimaryCacheTest()
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

  /**
   * Test of findInPrimary method, of class LocalPrimaryCache.
   * @throws java.lang.Exception
   */
  @Test
  public void testFindInPrimary1()
     throws Exception
  {
    System.out.println("findInPrimary1");
    String metaSchemaName = "STP";
    String metaTableName = "TRANSCODE";
    String metaColumnName = "APP";

    DatabaseMetaData meta = th.con.getMetaData();
    LocalPrimaryCache instance = new LocalPrimaryCache(th.con.getCatalog(), meta);
    int expResult = 1;
    int result = instance.findInPrimary(metaSchemaName, metaTableName, metaColumnName);
    assertEquals(expResult, result);
  }

  @Test
  public void testFindInPrimary2()
     throws Exception
  {
    System.out.println("findInPrimary2");
    String metaSchemaName = "";
    String metaTableName = "STP.TRANSCODE";
    String metaColumnName = "APP";

    DatabaseMetaData meta = th.con.getMetaData();
    LocalPrimaryCache instance = new LocalPrimaryCache(th.con.getCatalog(), meta);
    int expResult = 1;
    int result = instance.findInPrimary(metaSchemaName, metaTableName, metaColumnName);
    assertEquals(expResult, result);
  }
}
