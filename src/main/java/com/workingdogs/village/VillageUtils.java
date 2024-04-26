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

import java.io.OutputStream;
import java.io.Reader;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commonlib5.utils.CommonFileUtils;

/**
 *
 * @author Nicola De Nisco
 */
public class VillageUtils
{
  /** The log. */
  private static Log log = LogFactory.getLog(VillageUtils.class);

  /**
   * Ritorna il valore massimo di un campo su una tabella.
   *
   * @param tabella
   * @param campo
   * @param where
   * @param dbCon
   * @return
   * @throws Exception
   */
  public static long getMaxField(String tabella, String campo, String where, Connection dbCon)
     throws Exception
  {
    String sSQL = "SELECT MAX(" + campo + ") FROM " + tabella;
    if(where != null)
      sSQL += " WHERE " + where;

    Record rec = QueryDataSet.fetchFirstRecord(dbCon, sSQL);
    return rec == null ? 0 : rec.getValue(1).asLong();
  }

  public static long getValueFromSequence(String sequenceName, Connection con)
     throws Exception
  {
    String sSQL = "SELECT nextval('" + sequenceName + "'::regclass)";
    Record rec = QueryDataSet.fetchFirstRecord(con, sSQL);
    return rec == null ? 0 : rec.getValue(1).asLong();
  }

  public static int executeStatement(String sSQL, Connection con)
     throws Exception
  {
    try (Statement st = con.createStatement())
    {
      return st.executeUpdate(sSQL);
    }
  }

  /**
   * Esegue uno script SQL.
   * Ogni query viene riconosciuta dal terminatore ';'.
   * @param con connessione al db
   * @param r reader da cui leggere lo script
   * @param ignoreErrors se vero log degli errori senza interruzione
   * @return numero di query eseguite
   * @throws Exception
   */
  public static int executeSqlScript(Connection con, Reader r, boolean ignoreErrors)
     throws Exception
  {
    int c, count = 0;
    StringBuilder sb = new StringBuilder(128);

    do
    {
      c = r.read();

      if(c == ';' || c == -1)
      {
        if(sb.length() != 0)
        {
          String sSQL = sb.toString().trim();

          if(!sSQL.isEmpty())
          {
            try (PreparedStatement ps = con.prepareStatement(sSQL))
            {
              count += ps.executeUpdate();
            }
            catch(Exception ex)
            {
              if(ignoreErrors)
              {
                System.err.println("Execute script SQL error: " + ex.getMessage());
              }
              else
                throw ex;
            }
            sb = new StringBuilder(128);
          }
        }
      }
      else
      {
        sb.append((char) c);
      }
    }
    while(c != -1);

    return count;
  }

  /**
   * Private constructor to prevent instantiation.
   *
   * Class contains only static method ans should therefore not be
   * instantiated.
   */
  private VillageUtils()
  {
  }

  /**
   * Convenience Method to close a Table Data Set without
   * Exception check.
   *
   * @param tds A TableDataSet
   */
  public static final void close(final TableDataSet tds)
  {
    if(tds != null)
    {
      try
      {
        tds.close();
      }
      catch(Exception ignored)
      {
        log.debug("Caught exception when closing a TableDataSet",
           ignored);
      }
    }
  }

  /**
   * Convenience Method to close a Table Data Set without
   * Exception check.
   *
   * @param qds A TableDataSet
   */
  public static final void close(final QueryDataSet qds)
  {
    if(qds != null)
    {
      try
      {
        qds.close();
      }
      catch(Exception ignored)
      {
        log.debug("Caught exception when closing a QueryDataSet",
           ignored);
      }
    }
  }

  /**
   * Convenience Method to close an Output Stream without
   * Exception check.
   *
   * @param os An OutputStream
   */
  public static final void close(final OutputStream os)
  {
    try
    {
      if(os != null)
      {
        os.close();
      }
    }
    catch(Exception ignored)
    {
      log.debug("Caught exception when closing an OutputStream",
         ignored);
    }
  }

  /**
   * Converts a hashtable to a byte array for storage/serialization.
   *
   * @param hash The Hashtable to convert.
   * @return A byte[] with the converted Hashtable.
   * @throws Exception If an error occurs.
   */
  public static final byte[] hashtableToByteArray(final Hashtable hash)
     throws Exception
  {
    Hashtable saveData = new Hashtable(hash.size());

    // estrae solo le chiavi che implementano Serializable
    Iterator keys = hash.entrySet().iterator();
    while(keys.hasNext())
    {
      Map.Entry entry = (Map.Entry) keys.next();
      if(entry.getValue() instanceof Serializable)
      {
        saveData.put(entry.getKey(), entry.getValue());
      }
    }

    return CommonFileUtils.writeObjectToBytes(saveData);
  }

  /**
   * Factored out setting of a Village Record column from a generic value.
   *
   * @param value the value to set
   * @param rec The Village Record
   * @param colName The name of the column in the record
   * @throws java.lang.Exception
   */
  public static final void setVillageValue(Object value,
     final Record rec,
     final String colName)
     throws Exception
  {
    if(value == null)
    {
      rec.setValueNull(colName);
    }
    else if(value instanceof String)
    {
      rec.setValue(colName, (String) value);
    }
    else if(value instanceof Integer)
    {
      rec.setValue(colName, (int) value);
    }
    else if(value instanceof BigDecimal)
    {
      rec.setValue(colName, (BigDecimal) value);
    }
    else if(value instanceof Boolean)
    {
      rec.setValue(colName, ((Boolean) value).booleanValue());
    }
    else if(value instanceof java.util.Date)
    {
      rec.setValue(colName, (java.util.Date) value);
    }
    else if(value instanceof Float)
    {
      rec.setValue(colName, (float) value);
    }
    else if(value instanceof Double)
    {
      rec.setValue(colName, (double) value);
    }
    else if(value instanceof Byte)
    {
      rec.setValue(colName, ((Byte) value).byteValue());
    }
    else if(value instanceof Long)
    {
      rec.setValue(colName, (long) value);
    }
    else if(value instanceof Short)
    {
      rec.setValue(colName, ((Short) value).shortValue());
    }
    else if(value instanceof Hashtable)
    {
      rec.setValue(colName, hashtableToByteArray((Hashtable) value));
    }
    else if(value instanceof byte[])
    {
      rec.setValue(colName, (byte[]) value);
    }
  }

  public static Object mapVillageToObject(Value v)
     throws Exception
  {
    if(v.isBigDecimal())
    {
      return v.asBigDecimal();
    }
    else if(v.isByte())
    {
      return v.asInt();
    }
    else if(v.isBytes())
    {
      return null;
    }
    else if(v.isDate())
    {
      return v.asUtilDate();
    }
    else if(v.isShort())
    {
      return v.asInt();
    }
    else if(v.isInt())
    {
      return v.asInt();
    }
    else if(v.isLong())
    {
      return v.asInt();
    }
    else if(v.isDouble())
    {
      return v.asDouble();
    }
    else if(v.isFloat())
    {
      return v.asDouble();
    }
    else if(v.isBoolean())
    {
      return v.asBoolean();
    }
    else if(v.isString())
    {
      return v.asString();
    }
    else if(v.isTime())
    {
      return v.asUtilDate();
    }
    else if(v.isTimestamp())
    {
      return v.asUtilDate();
    }
    else if(v.isUtilDate())
    {
      return v.asUtilDate();
    }

    return v.toString();
  }

  public static int getFieldIndex(String fieldName, Record r)
  {
    try
    {
      return r.schema().index(fieldName);
    }
    catch(DataSetException ex)
    {
      return -1;
    }
  }

  public static java.sql.Date cvtDate(java.util.Date d)
  {
    return new java.sql.Date(d.getTime());
  }

  public static java.sql.Timestamp cvtTimestamp(java.util.Date d)
  {
    if(d == null)
      return null;

    return new java.sql.Timestamp(d.getTime());
  }

  public static java.sql.Timestamp cvtTimestampNotNull(java.util.Date d)
  {
    if(d == null)
      return currentTimestamp();

    return new java.sql.Timestamp(d.getTime());
  }

  public static java.sql.Timestamp currentTimestamp()
  {
    return new java.sql.Timestamp(System.currentTimeMillis());
  }
}
