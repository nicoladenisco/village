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

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.commonlib5.utils.ArrayOper;
import org.commonlib5.utils.Pair;
import org.commonlib5.utils.StringJoin;

/**
 * Helper per eseguire query con clausola in molto estesa.
 * La query viene eseguita spezzando la lista parametri in
 * sotto parti per non generare uno statemente ecessivamente lungo.<br>
 * Una query del tipo:
 * <pre>
 * SELECT * FROM stp.transcode WHERE app IN (###)
 * </pre>
 * viene eseguita più volte se la lista parametri supera maxQueryin (default 500).<br>
 * I risultati complessivi sono cumulati e recuperabili insieme allo schema.
 *
 * @author Nicola De Nisco
 * @param <T> tipo di dato; solo String, Integer, Long
 */
public class LargeInQuery<T>
{
  protected int maxQueryin = 500;
  protected String parmacro = "###";
  protected Class<? extends Object> objClass;
  protected Schema schema;
  protected final List<Record> lsr = new ArrayList<>(128);

  public LargeInQuery()
  {
  }

  public LargeInQuery(int maxQueryin, String parmacro)
  {
    this.maxQueryin = maxQueryin;
    this.parmacro = parmacro;
  }

  public int executeQuery(String sSQL, Connection con, T... parametri)
     throws Exception
  {
    return executeQuery(sSQL, Arrays.asList(parametri), con);
  }

  public int executeQuery(String sSQL, T[] parametri, Connection con)
     throws Exception
  {
    return executeQuery(sSQL, Arrays.asList(parametri), con);
  }

  public int executeQuery(String sSQL, int[] arId, Connection con)
     throws Exception
  {
    return executeQuery(sSQL, (List<T>) ArrayOper.asList(arId), con);
  }

  public int executeQuery(String sSQL, List<T> parametri, Connection con)
     throws Exception
  {
    if(parametri.isEmpty())
      return 0;

    objClass = parametri.get(0).getClass();
    return executeQuerySplit(sSQL, parametri, con);
  }

  protected int executeQuerySplit(String sSQL, List<T> parametri, Connection con)
     throws Exception
  {
    if(parametri.size() > maxQueryin)
    {
      int split = parametri.size() / 2;
      int p1 = executeQuery(sSQL, parametri.subList(0, split), con);
      int p2 = executeQuery(sSQL, parametri.subList(split, parametri.size()), con);
      return p1 + p2;
    }

    return executeQueryInternal(sSQL, parametri, con);
  }

  protected int executeQueryInternal(String sSQL, List<T> parametri, Connection con)
     throws Exception
  {
    String join = join(parametri);
    String sSQL1 = sSQL.replace(parmacro, join);

    if(schema == null)
    {
      Pair<Schema, List<Record>> p = QueryDataSet.fetchAllRecordsAndSchema(con, sSQL1);
      schema = p.first;
      lsr.addAll(p.second);
      return p.second.size();
    }
    else
    {
      List<Record> lsrecs = QueryDataSet.fetchAllRecords(con, sSQL1);
      lsr.addAll(lsrecs);
      return lsrecs.size();
    }
  }

  protected String join(List<T> parametri)
  {
    if(objClass.isAssignableFrom(String.class))
      return StringJoin.joinForSQLString((Collection<String>) parametri);
    else
      return StringJoin.joinForSQLNumber((Collection<Number>) parametri);
  }

  public Schema getSchema()
  {
    return schema;
  }

  public List<Record> getRecords()
  {
    return lsr;
  }

  public int getMaxQueryin()
  {
    return maxQueryin;
  }

  public void setMaxQueryin(int maxQueryin)
  {
    this.maxQueryin = maxQueryin;
  }

  public Pair<List<Record>, Schema> getRecordsAndSchema()
  {
    return new Pair<>(lsr, schema);
  }

  public Pair<Schema, List<Record>> getSchemaAndRecords()
  {
    return new Pair<>(schema, lsr);
  }

  public void clear()
  {
    schema = null;
    lsr.clear();
  }
}
