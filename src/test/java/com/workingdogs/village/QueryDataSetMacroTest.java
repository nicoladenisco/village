/*
 * Copyright (C) 2025 Nicola De Nisco
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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import org.commonlib5.utils.ArrayOper;
import org.commonlib5.utils.DateTime;
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
public class QueryDataSetMacroTest
{
  public final DerbyTestHelper th = new DerbyTestHelper();

  public QueryDataSetMacroTest()
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

  @BeforeClass
  public static void setUpClass()
  {
  }

  @AfterClass
  public static void tearDownClass()
  {
  }

  @Test
  public void testRE2()
     throws Exception
  {
    System.out.println("TEST REGEXP 2");
    String test = "${wapp:st}";
    Matcher m = QueryDataSetMacro.macroPatternParam.matcher(test);
    assertTrue(m.matches());
    assertEquals(2, m.groupCount());
    assertEquals("wapp", m.group(1));
    assertEquals("st", m.group(2));
  }

  @Test
  public void testrRsolveMacro2()
     throws Exception
  {
    System.out.println("TEST resolveMacro2()");
    String sSQL = "SELECT * FROM stp.transcode WHERE app=${wapp:st}";
    QueryDataSetMacro qds = new QueryDataSetMacro();
    String out = qds.resolveMacro2(sSQL);
    assertEquals(out, "SELECT * FROM stp.transcode WHERE app=?");
    assertEquals(qds.lsInfo.size(), 1);
    assertEquals(qds.lsInfo.get(0).toString(), "Info{first=38, macro=wapp, param=st}");
  }

  @Test
  public void test1()
     throws Exception
  {
    System.out.println("TEST GENERALE 1 - una stringa senza conversione di tipo");
    Map<String, String> params = ArrayOper.asMapFromPairStrings("wapp", "c");

    String sSQL = "SELECT * FROM stp.transcode WHERE app=${wapp}";
    try(QueryDataSetMacro qds = new QueryDataSetMacro(th.con, sSQL, params))
    {
      qds.fetchRecords();
      assertEquals(3, qds.size());

      Record r = qds.getRecord(0);
      assertNotNull(r);
      assertEquals(r.getValue(1).asString(), "c");
      assertEquals(r.getValue("app").asString(), "c");
    }
  }

  @Test
  public void test2()
     throws Exception
  {
    System.out.println("TEST GENERALE 2 - stringa con conversione di tipo");
    Map<String, String> params = ArrayOper.asMapFromPairStrings("wapp", "c");

    String sSQL = "SELECT * FROM stp.transcode WHERE app=${wapp:st}";
    try(QueryDataSetMacro qds = new QueryDataSetMacro(th.con, sSQL, params))
    {
      qds.fetchRecords();
      assertEquals(3, qds.size());

      Record r = qds.getRecord(0);
      assertNotNull(r);
      assertEquals(r.getValue(1).asString(), "c");
      assertEquals(r.getValue("app").asString(), "c");
    }
  }

  @Test
  public void test3()
     throws Exception
  {
    System.out.println("TEST GENERALE 3 - stringhe multiple");
    Map<String, String> params = ArrayOper.asMapFromPairStrings(
       "wapp", "c",
       "wcodapp", "CODAPP7",
       "wcodvero", "CODVERO7"
    );

    String sSQL = "SELECT * FROM stp.transcode WHERE app=${wapp:st} AND codice_app=${wcodapp} AND codice_vero=${wcodvero:str}";
    try(QueryDataSetMacro qds = new QueryDataSetMacro(th.con, sSQL, params))
    {
      qds.fetchRecords();
      assertEquals(1, qds.size());

      Record r = qds.getRecord(0);
      assertNotNull(r);
      assertEquals(r.getValue(1).asString(), "c");
      assertEquals(r.getValue("app").asString(), "c");
      assertEquals(r.getValue("codice_app").asString(), "CODAPP7");
      assertEquals(r.getValue("codice_vero").asString(), "CODVERO7");
    }
  }

  @Test
  public void test4()
     throws Exception
  {
    System.out.println("TEST GENERALE 4 - via prepared statement classico");
    long trifMilli = DateTime.convertDateCommonFormat("2025-01-01 08:47:00");

    String sSQL
       = "SELECT * FROM stp.transcode"
       + " WHERE app=?"
       + "   AND stato_rec > ?"
       + "   AND validita >= ?"
       + "   AND ult_modif >= ?"
       + " ORDER BY codice_vero";

    try(PreparedStatement ps = th.con.prepareStatement(sSQL))
    {
      ps.setString(1, "d");
      ps.setInt(2, 0);
      ps.setDate(3, new java.sql.Date(trifMilli));
      ps.setTimestamp(4, new Timestamp(trifMilli));

      try(QueryDataSet qds = new QueryDataSet(ps.executeQuery()))
      {
        qds.fetchRecords();
        assertEquals(3, qds.size());

        Record r = qds.getRecord(0);
        assertNotNull(r);
        assertEquals(r.getValue(1).asString(), "d");
        assertEquals(r.getValue("app").asString(), "d");
        assertEquals(r.getValue("codice_app").asString(), "CODAPP11");
        assertEquals(r.getValue("codice_vero").asString(), "CODVER11");
      }
    }
  }

  @Test
  public void test5()
     throws Exception
  {
    System.out.println("TEST GENERALE 5 - conversioni di tipo");
    Map<String, String> params = ArrayOper.asMapFromPairStrings(
       "wapp", "d",
       "statorec", "0",
       "validita", "2025-11-01",
       "um", "2025-11-01 08:47:00"
    );

    String sSQL
       = "SELECT * FROM stp.transcode"
       + " WHERE app=${wapp}"
       + "   AND stato_rec > ${statorec:int}"
       + "   AND validita >= ${validita:date}"
       + "   AND ult_modif >= ${um:ts}"
       + " ORDER BY codice_vero";

    try(QueryDataSetMacro qds = new QueryDataSetMacro(th.con, sSQL, params))
    {
      qds.fetchRecords();
      assertEquals(3, qds.size());

      Record r = qds.getRecord(0);
      assertNotNull(r);
      assertEquals(r.getValue(1).asString(), "d");
      assertEquals(r.getValue("app").asString(), "d");
      assertEquals(r.getValue("codice_app").asString(), "CODAPP11");
      assertEquals(r.getValue("codice_vero").asString(), "CODVER11");
    }
  }

  @Test
  public void test6()
     throws Exception
  {
    System.out.println("TEST GENERALE 6 - tipi nativi con conversione di tipo");
    long trifMilli = DateTime.convertDateCommonFormat("2025-01-01 08:47:00");

    Map<String, Object> params = new HashMap<>();
    params.put("wapp", "d");
    params.put("statorec", 0);
    params.put("validita", new Date(trifMilli));
    params.put("um", new Timestamp(trifMilli));

    String sSQL
       = "SELECT * FROM stp.transcode"
       + " WHERE app=${wapp}"
       + "   AND stato_rec > ${statorec:int}"
       + "   AND validita >= ${validita:date}"
       + "   AND ult_modif >= ${um:ts}"
       + " ORDER BY codice_vero";

    try(QueryDataSetMacro qds = new QueryDataSetMacro(th.con, sSQL, params))
    {
      qds.fetchRecords();
      assertEquals(3, qds.size());

      Record r = qds.getRecord(0);
      assertNotNull(r);
      assertEquals(r.getValue(1).asString(), "d");
      assertEquals(r.getValue("app").asString(), "d");
      assertEquals(r.getValue("codice_app").asString(), "CODAPP11");
      assertEquals(r.getValue("codice_vero").asString(), "CODVER11");
    }
  }

  @Test
  public void test7()
     throws Exception
  {
    System.out.println("TEST GENERALE 7 - tipi nativi senza conversione di tipo");
    long trifMilli = DateTime.convertDateCommonFormat("2025-01-01 08:47:00");

    Map<String, Object> params = new HashMap<>();
    params.put("wapp", "d");
    params.put("statorec", 0);
    params.put("validita", new Date(trifMilli));
    params.put("um", new Timestamp(trifMilli));

    String sSQL
       = "SELECT * FROM stp.transcode"
       + " WHERE app=${wapp}"
       + "   AND stato_rec > ${statorec}"
       + "   AND validita >= ${validita}"
       + "   AND ult_modif >= ${um}"
       + " ORDER BY codice_vero";

    try(QueryDataSetMacro qds = new QueryDataSetMacro(th.con, sSQL, params))
    {
      qds.fetchRecords();
      assertEquals(3, qds.size());

      Record r = qds.getRecord(0);
      assertNotNull(r);
      assertEquals(r.getValue(1).asString(), "d");
      assertEquals(r.getValue("app").asString(), "d");
      assertEquals(r.getValue("codice_app").asString(), "CODAPP11");
      assertEquals(r.getValue("codice_vero").asString(), "CODVER11");
    }
  }

  protected String __function;
  protected Object[] __args;

  protected void setFunArgs(String function, Object[] args)
  {
    this.__function = function;
    this.__args = args;
  }

  @Test
  public void test_setPreparedStatementField()
     throws Exception
  {
    System.out.println("TEST setPreparedStatementField()");
    QueryDataSetMacro qds = new QueryDataSetMacro();
    PreparedStatement ps = createAdapter();

    int c = 1;

    String val = "prova", fun = "setString";
    testPS(qds, ps, c++, "st", val, fun);
    testPS(qds, ps, c++, "str", val, fun);
    testPS(qds, ps, c++, "String", val, fun);

    testPS(qds, ps, c++, "int", "1", 1, "setInt");
    testPS(qds, ps, c++, "Integer", 1, "setInt");
    testPS(qds, ps, c++, "long", "1", 1L, "setLong");
    testPS(qds, ps, c++, "Long", 1L, "setLong");
    testPS(qds, ps, c++, "float", "10.0", 10.0f, "setFloat");
    testPS(qds, ps, c++, "Float", 10.0f, "setFloat");
    testPS(qds, ps, c++, "number", "10", 10.0, "setDouble");
    testPS(qds, ps, c++, "currency", "10", 10.0, "setDouble");
    testPS(qds, ps, c++, "double", "10", 10.0, "setDouble");
    testPS(qds, ps, c++, "Double", 10.0, "setDouble");
    testPS(qds, ps, c++, "boolean", "true", Boolean.TRUE, "setBoolean");
    testPS(qds, ps, c++, "Boolean", true, "setBoolean");

    final Timestamp tstest = new Timestamp(System.currentTimeMillis());
    final Date datetest = new java.sql.Date(System.currentTimeMillis());
    final String today = DateTime.formatIsoFull(datetest);

    testPSDate(qds, ps, c++, "ts", today, tstest, "setTimestamp");
    testPSDate(qds, ps, c++, "timestamp", today, tstest, "setTimestamp");
    testPSDate(qds, ps, c++, "Timestamp", tstest, tstest, "setTimestamp");
    testPSDate(qds, ps, c++, "date", today, datetest, "setDate");
    testPSDate(qds, ps, c++, "Date", datetest, datetest, "setDate");
    testPSDate(qds, ps, c++, "UDate", datetest, datetest, "setDate");
  }

  private void testPS(QueryDataSetMacro qds, PreparedStatement ps, int c, String tipo, Object value, String funzione)
     throws SQLException
  {
    testPS(qds, ps, c, tipo, value, value, funzione);
  }

  private void testPS(QueryDataSetMacro qds, PreparedStatement ps, int c, String tipo, Object value, Object valueTest, String funzione)
     throws SQLException
  {
    __function = null;
    __args = null;
    qds.setPreparedStatementField(ps, c, tipo, value);
    assertEquals(funzione, __function);
    assertEquals(2, __args.length);
    long valueCount = ((Integer) __args[0]);
    assertEquals(c, valueCount);
    assertEquals(valueTest, __args[1]);
    System.out.println("tipo=" + tipo + " passed");
  }

  private void testPSDate(QueryDataSetMacro qds, PreparedStatement ps, int c, String tipo, Object value, Object valueTest, String funzione)
     throws SQLException
  {
    __function = null;
    __args = null;
    qds.setPreparedStatementField(ps, c, tipo, value);
    assertEquals(funzione, __function);
    assertEquals(2, __args.length);
    long valueCount = ((Integer) __args[0]);
    assertEquals(c, valueCount);
    assertEquals(valueTest.getClass(), __args[1].getClass());

    String se = valueTest.toString();
    String sv = __args[1].toString();
    int pos;
    if((pos = se.indexOf('.')) != -1)
      se = se.substring(0, pos);
    if((pos = sv.indexOf('.')) != -1)
      sv = sv.substring(0, pos);

    assertEquals(se, sv);
    System.out.println("tipo=" + tipo + " passed");
  }

  public PreparedStatement createAdapter()
  {
    return (PreparedStatement) Proxy.newProxyInstance(
       QueryDataSetMacroTest.class.getClassLoader(),
       new Class<?>[]
       {
         PreparedStatement.class
       },
       new LocalHandler());
  }

  public class LocalHandler implements InvocationHandler
  {
    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
       throws Throwable
    {
      final String name = method.getName();
      if(!"toString".equals(name))
        setFunArgs(name, args);
      return null;
    }
  }

  @Test
  public void testIterable()
     throws Exception
  {
    System.out.println("testIterable");
    long trifMilli = DateTime.convertDateCommonFormat("2025-01-01 08:47:00");

    Map<String, Object> params = new HashMap<>();
    params.put("wapp", "d");
    params.put("statorec", 0);
    params.put("validita", new Date(trifMilli));
    params.put("um", new Timestamp(trifMilli));

    String sSQL
       = "SELECT * FROM stp.transcode"
       + " WHERE app=${wapp}"
       + "   AND stato_rec > ${statorec}"
       + "   AND validita >= ${validita}"
       + "   AND ult_modif >= ${um}"
       + " ORDER BY codice_vero";

    int count1 = 0, count2 = 0;
    try(QueryDataSetMacro qds = new QueryDataSetMacro(th.con, sSQL, params))
    {
      for(Record r : qds)
      {
        System.out.println("R1=" + r);
        count1++;
      }

      for(Record r : qds)
      {
        System.out.println("R2=" + r);
        count2++;
      }
    }

    assertEquals(3, count1);
    assertEquals(3, count2);
  }
}
