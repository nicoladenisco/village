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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.commonlib5.utils.ArrayMap;
import org.commonlib5.utils.DateTime;
import org.commonlib5.utils.Pair;
import org.commonlib5.utils.StringOper;

/**
 * Query dataset con risoluzione di macro.
 * I parametri della query possono essere specificati sotto forma di macro.
 * <pre>
 * Map<String, String> params = ArrayOper.asMapFromPairStrings(
 *    "wapp", "d",
 *    "statorec", "0",
 *    "validita", "2025-11-01",
 *    "um", "2025-11-01 08:47:00"
 * );
 *
 * String sSQL
 *    = "SELECT * FROM stp.transcode"
 *    + " WHERE app=${wapp}"
 *    + "   AND stato_rec > ${statorec:int}"
 *    + "   AND validita >= ${validita:date}"
 *    + "   AND ult_modif >= ${um:ts}"
 *    + " ORDER BY codice_vero";
 *
 * try(QueryDataSetMacro qds = new QueryDataSetMacro(th.con, sSQL, params))
 * {
 *     ...
 * }
 * </pre>
 * Le macro sono nella forma ${nome_macro}. Il tipo viene determinato
 * dal valore inserito nella map dei parametri.<br>
 * Volendo è possibile forzare una conversione sul tipo usando la sintassi ${nome_macro:tipo}
 * dove tipo può essere una valore come dalla mappa:<br>
 * <pre>
 * TIPI PURI (conversione non necessaria)
 * String.class, "String"
 * Integer.class, "Integer"
 * Long.class, "Long"
 * Float.class, "Float"
 * Double.class, "Double"
 * Boolean.class, "Boolean"
 * Timestamp.class, "Timestamp"
 * java.sql.Date.class, "Date"
 * java.util.Date.class, "UDate"
 *
 * TIPI CON CONVERSIONE
 * "st":"String"
 * "str":"String"
 *
 * "int":"Integer"
 *
 * "long":"Long"
 *
 * "float":"Float"
 *
 * "number":
 * "currency":
 * "double":"Double"
 *
 * "boolean":"Boolean"
 *
 * "ts":
 * "timestamp":"java.sql.Timestamp"
 *
 * "date":"java.sql.Date"
 *
 * </pre>
 * Nei tipi con conversione il valore viene convertito se possibile nel relativo tipo destinazione.<br>
 * Per le date e i timestamp sono supportati i formati (nell'ordine di conversione):
 * <ul>
 * <li>ISOformatFull (yyyy-MM-dd HH:mm:ss)</li>
 * <li>dfDataOra (yyyyMMdd HHmmss)</li>
 * <li>dfDTMXDS (yyyyMMddHHmmss)</li>
 * <li>ISOformat (yyyy-MM-dd)</li>
 * <li>dfData (yyyyMMdd)</li>
 * </ul>
 * @author Nicola De Nisco
 */
public class QueryDataSetMacro extends QueryDataSet
{
  public static final Pattern macroPattern = Pattern.compile("\\$\\{([a-z|A-Z|0-9|_|\\.]+)\\}");
  public static final Pattern macroPatternParam = Pattern.compile("\\$\\{([a-z|A-Z|0-9|_|\\.]+)\\:([a-z|A-Z]+)\\}");
  public final List<Info> lsInfo = new ArrayList<>();
  public final Map<String, Object> parMap = new HashMap<>();

  public QueryDataSetMacro()
     throws SQLException, DataSetException
  {
  }

  public QueryDataSetMacro(Connection conn, String selectStmt, Map<String, ? extends Object> values)
     throws SQLException, DataSetException
  {
    parMap.putAll(values);
    String sql1 = resolveMacro1(selectStmt);
    String sql2 = resolveMacro2(sql1);

    if(sql2.contains("${"))
      throw new DataSetException("Unresolved macro in sql (" + sql2 + ")");

    if(!lsInfo.isEmpty())
      lsInfo.sort((i1, i2) -> Integer.compare(i1.first, i2.first));

    //System.out.println("sql=" + sql2);
    //System.out.println("info=" + lsInfo);
    this.conn = conn;
    selectString = new StringBuilder(sql2);

    boolean ok = false;
    try
    {
      openResultset();
      schema = new Schema();
      schema.populate(resultSet.getMetaData(), null, null, conn);
      ok = true;
    }
    finally
    {
      if(!ok)
      {
        try
        {
          close();
        }
        catch(Exception ignored)
        {
          // ignore as another exception is already thrown
        }
      }
    }
  }

  @Override
  protected void openResultset()
     throws SQLException, DataSetException
  {
    stmt = conn.prepareStatement(selectString.toString(),
       ResultSet.TYPE_SCROLL_INSENSITIVE, // Permette di scorrere avanti/indietro
       ResultSet.CONCUR_READ_ONLY // Solo lettura
    );

    mergeParams();
    resultSet = ((PreparedStatement) stmt).executeQuery();
  }

  protected String resolveMacro1(String seg)
     throws DataSetException
  {
    Matcher m = macroPattern.matcher(seg);

    StringBuffer sb = new StringBuffer();
    while(m.find())
    {
      if(m.groupCount() != 1)
        throw new DataSetException("Syntax error in " + seg);

      String keyMacro = m.group(1);
      lsInfo.add(new Info(m.start(), keyMacro, "auto"));
      m.appendReplacement(sb, "?");
    }
    m.appendTail(sb);

    return sb.toString();
  }

  protected String resolveMacro2(String seg)
     throws DataSetException
  {
    Matcher m = macroPatternParam.matcher(seg);

    StringBuffer sb = new StringBuffer();
    while(m.find())
    {
      if(m.groupCount() != 2)
        throw new DataSetException("Syntax error in " + seg);

      String keyMacro = m.group(1);
      String parMacro = m.group(2);
      lsInfo.add(new Info(m.start(), keyMacro, parMacro));
      m.appendReplacement(sb, "?");
    }
    m.appendTail(sb);

    return sb.toString();
  }

  private void mergeParams()
     throws DataSetException, SQLException
  {
    PreparedStatement ps = (PreparedStatement) stmt;

    int c = 1;
    for(Info i : lsInfo)
    {
      Object value = parMap.get(i.macro);
      if(value == null)
        throw new DataSetException("Missing value for macro {" + i.macro + "}.");

      String tipo = i.param;
      if("auto".equals(tipo))
        tipo = detectParam(i, value);

      setPreparedStatementField(ps, c++, tipo, value);
    }
  }

  public void setPreparedStatementField(PreparedStatement ps, int c, String tipo, Object value)
     throws SQLException
  {
    switch(tipo)
    {
      default:
      case "st":
      case "str":
      case "String":
        ps.setString(c, StringOper.okStr(value));
        break;
      case "int":
        if(!Number.class.isAssignableFrom(value.getClass()))
          value = StringOper.parse(value, 0);
      case "Integer":
        ps.setInt(c, ((Number) value).intValue());
        break;
      case "long":
        if(!Number.class.isAssignableFrom(value.getClass()))
          value = StringOper.parse(value, 0.0);
      case "Long":
        ps.setLong(c, ((Number) value).longValue());
        break;
      case "float":
        if(!Number.class.isAssignableFrom(value.getClass()))
          value = StringOper.parse(value, 0.0);
      case "Float":
        ps.setFloat(c, ((Number) value).floatValue());
        break;
      case "number":
      case "currency":
      case "double":
        if(!Number.class.isAssignableFrom(value.getClass()))
          value = StringOper.parse(value, 0.0);
      case "Double":
        ps.setDouble(c, ((Number) value).doubleValue());
        break;
      case "boolean":
      case "Boolean":
        ps.setBoolean(c, StringOper.checkTrueFalse(value, false));
        break;
      case "ts":
      case "timestamp":
        if(!Timestamp.class.isAssignableFrom(value.getClass()))
        {
          if(value instanceof java.util.Date)
            value = new Timestamp(((java.util.Date) value).getTime());
          else
            value = new Timestamp(convertDateCommonFormat(value.toString()));
        }
      case "Timestamp":
        ps.setTimestamp(c, (Timestamp) value);
        break;
      case "date":
        if(!java.sql.Date.class.isAssignableFrom(value.getClass()))
        {
          if(value instanceof java.util.Date)
            value = new java.sql.Date(((java.util.Date) value).getTime());
          else
            value = new java.sql.Date(convertDateCommonFormat(value.toString()));
        }
      case "Date":
        ps.setDate(c, (java.sql.Date) value);
        break;
      case "UDate":
        ps.setDate(c, new java.sql.Date(((java.util.Date) value).getTime()));
        break;
    }
  }

  private String detectParam(Info i, Object value)
     throws DataSetException
  {
    Class vc = value.getClass();
    for(Pair<Class, String> type : types.getAsList())
    {
      if(type.first.isAssignableFrom(vc))
        return type.second;
    }

    throw new DataSetException("Illegal value for macro {" + i.macro + "=" + value + "}.");
  }

  /**
   * Try to convert from common date format.
   * @param s
   * @return
   */
  protected long convertDateCommonFormat(String s)
  {
    long rv = DateTime.convertDateCommonFormat(s);

    if(rv == 0)
      throw new IllegalArgumentException("Unrecognized date/time format.");

    return rv;
  }

  public static final ArrayMap<Class, String> types = new ArrayMap<>();

  static
  {
    types.put(String.class, "String");
    types.put(Integer.class, "Integer");
    types.put(Long.class, "Long");
    types.put(Float.class, "Float");
    types.put(Double.class, "Double");
    types.put(Boolean.class, "Boolean");
    types.put(Timestamp.class, "Timestamp");
    types.put(java.sql.Date.class, "Date");
    types.put(java.util.Date.class, "UDate");
  }

  public static class Info
  {
    public int first;
    public String macro, param;

    public Info(int first, String macro, String param)
    {
      this.first = first;
      this.macro = macro;
      this.param = param;
    }

    @Override
    public String toString()
    {
      return "Info{" + "first=" + first + ", macro=" + macro + ", param=" + param + '}';
    }
  }
}
