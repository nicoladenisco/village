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
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Nicola De Nisco
 */
public class KeyDefTest
{

  public KeyDefTest()
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

  /**
   * Test of addAttrib method, of class KeyDef.
   */
  @Test
  public void testAddAttrib()
  {
    System.out.println("addAttrib");
    String name = "pippo";
    KeyDef instance = new KeyDef();
    instance.addAttrib(name);
    assertTrue(instance.containsAttrib(name));
  }

  /**
   * Test of getAttrib method, of class KeyDef.
   */
  @Test
  public void testGetAttrib()
  {
    System.out.println("getAttrib");
    int pos = 0;
    KeyDef instance = new KeyDef();
    instance.addAttrib("pippo");
    String result = instance.getAttrib(pos);
    assertEquals("pippo", result);
  }
}
