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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A KeyDef is a way to define the key columns in a table. The KeyDef is generally used in conjunction with a <a
 * href="TableDataSet.html">TableDataSet</a>. Essentially a KeyDef is what forms the WHERE clause for an UPDATE or
 * DELETE.
 *
 * <br>
 * In order to use the KeyDef, you simply use it like this:
 * <PRE>
 *  KeyDef kd = new KeyDef().addAttrib("key_column_a");
 *  TableDataSet tds = new TableDataSet ( connection, "table", kd );
 *  tds.fetchRecords();
 *  Record rec = tds.getRecord(0);
 *  rec.setValue("column_name", "new value" );
 *  rec.save();
 *  tds.close();
 * </PRE>
 * In the above example, Record 0 is retrieved from the database table and the following update statement is generated:
 * <br>
 *
 * UPDATE table SET column_name=? WHERE key_column_a=?
 *
 *
 * @author <a href="mailto:jon@latchkey.com">Jon S. Stevens</a>
 * @version $Revision: 568 $
 */
public class KeyDef
{
  private final ArrayList<String> mySelf = new ArrayList<>();

  /**
   * Constructor for KeyDef.
   * Make sure to always initialize KeyDef with an initial element because it is 1 based.
   */
  public KeyDef()
  {
    mySelf.add("");
  }

  /**
   * Constructor for KeyDef.
   * @param keyNames list of names of field in primary key.
   */
  public KeyDef(String... keyNames)
  {
    mySelf.add("");
    mySelf.addAll(Arrays.asList(keyNames));
  }

  /**
   * Adds the named attribute to the KeyDef.
   *
   * @param name TODO: DOCUMENT ME!
   *
   * @return a copy of itself
   */
  public KeyDef addAttrib(String name)
  {
    mySelf.add(name);

    return this;
  }

  /**
   * Determines if the KeyDef contains the requested Attribute.
   *
   * @param name TODO: DOCUMENT ME!
   *
   * @return true if the attribute has been defined. false otherwise.
   */
  public boolean containsAttrib(String name)
  {
    return (mySelf.indexOf(name) != -1);
  }

  /**
   * getAttrib is 1 based. Setting pos to 0 will attempt to return pos 1.
   *
   * @param pos TODO: DOCUMENT ME!
   *
   * @return value of Attribute at pos as String. null if value is not found.
   */
  public String getAttrib(int pos)
  {
    if(pos == 0)
    {
      pos = 1;
    }

    try
    {
      return (String) mySelf.get(pos);
    }
    catch(ArrayIndexOutOfBoundsException e)
    {
      return null;
    }
  }

  public boolean isEmpty()
  {
    return mySelf.size() <= 1;
  }

  /**
   * KeyDef's are 1 based, returns size - 1
   *
   * @return the number of elements in the KeyDef that were set by addAttrib()
   *
   * @see #addAttrib(java.lang.String)
   */
  public int size()
  {
    return mySelf.size() - 1;
  }

  public List<String> getAsList()
  {
    if(isEmpty())
      return Collections.EMPTY_LIST;

    return mySelf.subList(1, mySelf.size());
  }

  @Override
  public String toString()
  {
    if(isEmpty())
      return "KeyDef{empty}";

    return "KeyDef{" + "mySelf=" + getAsList() + '}';
  }
}
