/*
 * Copyright (C) 2026 Nicola De Nisco
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

import java.util.List;
import org.commonlib5.utils.Pair;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test per LargeInQuery.
 *
 * @author Nicola De Nisco
 */
public class LargeInQueryTest
{
  public final DerbyTestHelper th = new DerbyTestHelper();

  public LargeInQueryTest()
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
  public void testGetRecordsAndSchema()
     throws Exception
  {
    System.out.println("getRecordsAndSchema");
    LargeInQuery<String> instance = new LargeInQuery<>();
    final String sSQL = "SELECT * FROM stp.transcode WHERE app IN (###)";
    int num = instance.executeQuery(sSQL, th.con, "a", "b", "c");
    Pair<Schema, List<Record>> result = instance.getSchemaAndRecords();

    assertTrue(num > 0);
    assertTrue(result.first != null);
    assertTrue(result.second.size() > 0);
  }
}
