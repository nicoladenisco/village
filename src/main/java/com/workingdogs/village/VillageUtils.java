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

import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 *
 * @author Nicola De Nisco
 */
public class VillageUtils
{
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
}
