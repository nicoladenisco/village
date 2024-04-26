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
import org.junit.Before;
import org.junit.BeforeClass;

/**
 *
 * @author Nicola De Nisco
 */
public class ColumnTest
{

  public ColumnTest()
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
  {
  }

  @After
  public void tearDown()
  {
  }

//  /**
//   * Test of populate method, of class Column.
//   */
//  @Test
//  public void testPopulate_5args()
//     throws Exception
//  {
//    System.out.println("populate");
//    ResultSetMetaData rsmd = null;
//    int colNum = 0;
//    String tableName = "";
//    String columnName = "";
//    int primaryIndex = 0;
//    Column instance = new Column();
//    instance.populate(rsmd, colNum, tableName, columnName, primaryIndex);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of populate method, of class Column.
//   */
//  @Test
//  public void testPopulate_6args()
//  {
//    System.out.println("populate");
//    String tableName = "";
//    String columnName = "";
//    String columnTypeName = "";
//    int columnType = 0;
//    boolean isNullable = false;
//    int primaryIndex = 0;
//    Column instance = new Column();
//    instance.populate(tableName, columnName, columnTypeName, columnType, isNullable, primaryIndex);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of name method, of class Column.
//   */
//  @Test
//  public void testName()
//  {
//    System.out.println("name");
//    Column instance = new Column();
//    String expResult = "";
//    String result = instance.name();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of typeEnum method, of class Column.
//   */
//  @Test
//  public void testTypeEnum()
//  {
//    System.out.println("typeEnum");
//    Column instance = new Column();
//    int expResult = 0;
//    int result = instance.typeEnum();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of typeName method, of class Column.
//   */
//  @Test
//  public void testTypeName()
//  {
//    System.out.println("typeName");
//    Column instance = new Column();
//    String expResult = "";
//    String result = instance.typeName();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of nullAllowed method, of class Column.
//   */
//  @Test
//  public void testNullAllowed()
//  {
//    System.out.println("nullAllowed");
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.nullAllowed();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of readOnly method, of class Column.
//   */
//  @Test
//  public void testReadOnly()
//  {
//    System.out.println("readOnly");
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.readOnly();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of getPrimaryIndex method, of class Column.
//   */
//  @Test
//  public void testGetPrimaryIndex()
//  {
//    System.out.println("getPrimaryIndex");
//    Column instance = new Column();
//    int expResult = 0;
//    int result = instance.getPrimaryIndex();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of isPrimaryKey method, of class Column.
//   */
//  @Test
//  public void testIsPrimaryKey()
//  {
//    System.out.println("isPrimaryKey");
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.isPrimaryKey();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of type method, of class Column.
//   */
//  @Test
//  public void testType()
//  {
//    System.out.println("type");
//    Column instance = new Column();
//    String expResult = "";
//    String result = instance.type();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of isBoolean method, of class Column.
//   */
//  @Test
//  public void testIsBoolean()
//  {
//    System.out.println("isBoolean");
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.isBoolean();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of isBigDecimal method, of class Column.
//   */
//  @Test
//  public void testIsBigDecimal()
//  {
//    System.out.println("isBigDecimal");
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.isBigDecimal();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of isBinary method, of class Column.
//   */
//  @Test
//  public void testIsBinary()
//  {
//    System.out.println("isBinary");
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.isBinary();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of isByte method, of class Column.
//   */
//  @Test
//  public void testIsByte()
//  {
//    System.out.println("isByte");
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.isByte();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of isBytes method, of class Column.
//   */
//  @Test
//  public void testIsBytes()
//  {
//    System.out.println("isBytes");
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.isBytes();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of isDate method, of class Column.
//   */
//  @Test
//  public void testIsDate()
//  {
//    System.out.println("isDate");
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.isDate();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of isDouble method, of class Column.
//   */
//  @Test
//  public void testIsDouble()
//  {
//    System.out.println("isDouble");
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.isDouble();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of isFloat method, of class Column.
//   */
//  @Test
//  public void testIsFloat()
//  {
//    System.out.println("isFloat");
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.isFloat();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of isInt method, of class Column.
//   */
//  @Test
//  public void testIsInt()
//  {
//    System.out.println("isInt");
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.isInt();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of isLong method, of class Column.
//   */
//  @Test
//  public void testIsLong()
//  {
//    System.out.println("isLong");
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.isLong();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of isShort method, of class Column.
//   */
//  @Test
//  public void testIsShort()
//  {
//    System.out.println("isShort");
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.isShort();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of isString method, of class Column.
//   */
//  @Test
//  public void testIsString()
//  {
//    System.out.println("isString");
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.isString();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of isTime method, of class Column.
//   */
//  @Test
//  public void testIsTime()
//  {
//    System.out.println("isTime");
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.isTime();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of isTimestamp method, of class Column.
//   */
//  @Test
//  public void testIsTimestamp()
//  {
//    System.out.println("isTimestamp");
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.isTimestamp();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of isVarBinary method, of class Column.
//   */
//  @Test
//  public void testIsVarBinary()
//  {
//    System.out.println("isVarBinary");
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.isVarBinary();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of isLongVarBinary method, of class Column.
//   */
//  @Test
//  public void testIsLongVarBinary()
//  {
//    System.out.println("isLongVarBinary");
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.isLongVarBinary();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of isNumericValue method, of class Column.
//   */
//  @Test
//  public void testIsNumericValue()
//  {
//    System.out.println("isNumericValue");
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.isNumericValue();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of isStringValue method, of class Column.
//   */
//  @Test
//  public void testIsStringValue()
//  {
//    System.out.println("isStringValue");
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.isStringValue();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of isDateValue method, of class Column.
//   */
//  @Test
//  public void testIsDateValue()
//  {
//    System.out.println("isDateValue");
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.isDateValue();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of getTableName method, of class Column.
//   */
//  @Test
//  public void testGetTableName()
//  {
//    System.out.println("getTableName");
//    Column instance = new Column();
//    String expResult = "";
//    String result = instance.getTableName();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of toString method, of class Column.
//   */
//  @Test
//  public void testToString()
//  {
//    System.out.println("toString");
//    Column instance = new Column();
//    String expResult = "";
//    String result = instance.toString();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of hashCode method, of class Column.
//   */
//  @Test
//  public void testHashCode()
//  {
//    System.out.println("hashCode");
//    Column instance = new Column();
//    int expResult = 0;
//    int result = instance.hashCode();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of equals method, of class Column.
//   */
//  @Test
//  public void testEquals()
//  {
//    System.out.println("equals");
//    Object obj = null;
//    Column instance = new Column();
//    boolean expResult = false;
//    boolean result = instance.equals(obj);
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
}
