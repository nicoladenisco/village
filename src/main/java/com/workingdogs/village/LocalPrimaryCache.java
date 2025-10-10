/*
 * Copyright (C) 2022 Nicola De Nisco
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package com.workingdogs.village;

import static com.workingdogs.village.Schema.TABLES_FILTER;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;
import org.commonlib5.utils.Pair;
import org.commonlib5.utils.StringOper;

/**
 * Cache delle chiavi primarie.
 * Viene utilizzata in Schema per stabilire se una colonna è chiave primaria.
 *
 * @author Nicola De Nisco
 */
public class LocalPrimaryCache
{
  private final String catalog, connURL;
  private final DatabaseMetaData dbMeta;
  private static final TreeMap<String, Map<String, Integer>> pkCache = new TreeMap<>((s1, s2) -> s1.compareToIgnoreCase(s2));
  private static final Object semaforo = new Object();
  private int sintassi = 0;

  public LocalPrimaryCache(String catalog, DatabaseMetaData dbMeta)
     throws SQLException
  {
    this.dbMeta = dbMeta;
    this.catalog = catalog;
    this.connURL = dbMeta.getURL();

    if(dbMeta.getClass().getName().contains("Jtds"))
      sintassi = 1;
  }

  public int findInPrimary(String metaSchemaName, String metaTableName, String metaColumnName)
     throws SQLException
  {
    String key = makeKey(metaSchemaName, metaTableName);
    Map<String, Integer> tablepks = pkCache.get(key);
    if(tablepks != null)
      return tablepks.getOrDefault(metaColumnName, 0);

    if(sintassi != 1)
    {
      int pos;
      if(metaSchemaName.isEmpty() && (pos = metaTableName.indexOf('.')) != -1)
      {
        // workaround nel caso metaTableName è nella forma SCHEMA.TABELLA
        metaSchemaName = metaTableName.substring(0, pos);
        metaTableName = metaTableName.substring(pos + 1);
      }
    }

    // corregge il case dei nomi: è critico per le ricerche
    Pair<String, String> nomiCorretti = correggiCase(metaSchemaName, metaTableName);

    tablepks = creaInfoPerTabella(key, nomiCorretti, metaColumnName);
    return tablepks.getOrDefault(metaColumnName, 0);
  }

  protected Map<String, Integer> creaInfoPerTabella(String key, Pair<String, String> nomi, String metaColumnName)
     throws SQLException
  {
    Map<String, Integer> tablepks;

    synchronized(semaforo)
    {
      // ripete ricerca dopo aver acquisito il semaforo
      tablepks = pkCache.get(key);

      if(tablepks == null)
      {
        tablepks = new TreeMap<>((s1, s2) -> s1.compareToIgnoreCase(s2));

        switch(sintassi)
        {
          case 1:
            jtdsDriver(nomi.first, nomi.second, tablepks);
            break;

          default:
            allDriver(nomi.first, nomi.second, tablepks);
            break;
        }

        pkCache.put(key, tablepks);
      }
    }

    return tablepks;
  }

  protected String makeKey(String metaSchemaName, String metaTableName)
  {
    return connURL + "|" + StringOper.okStr(metaSchemaName, "NO_SCHEMA") + "|" + metaTableName;
  }

  protected void allDriver(String metaSchemaName, String metaTableName, Map<String, Integer> tablepks)
     throws SQLException
  {
    try(ResultSet dbPrimary = dbMeta.getPrimaryKeys(catalog, metaSchemaName, metaTableName))
    {
      while(dbPrimary.next())
      {
        String nomeColonna = dbPrimary.getString("COLUMN_NAME");
        int kinfo = dbPrimary.getInt("KEY_SEQ");
        tablepks.put(nomeColonna, kinfo);
      }
    }
  }

  /**
   * Il db Microsoft SQL ha una gestione particolare.
   * @param metaSchemaName
   * @param metaTableName
   * @param tablepks
   * @throws SQLException
   */
  protected void jtdsDriver(String metaSchemaName, String metaTableName, Map<String, Integer> tablepks)
     throws SQLException
  {
    String sc = catalog;
    int pos = metaTableName.indexOf("..");
    if(pos != -1)
    {
      sc = metaTableName.substring(0, pos);
      metaTableName = metaTableName.substring(pos + 2);
    }

    try(ResultSet dbPrimary = dbMeta.getPrimaryKeys(sc, "dbo", metaTableName))
    {
      while(dbPrimary.next())
      {
        String nomeColonna = dbPrimary.getString("COLUMN_NAME");
        int kinfo = dbPrimary.getInt("KEY_SEQ");
        tablepks.put(nomeColonna, kinfo);
      }
    }
  }

  /**
   * Correzione dei case di schema e tabella.
   * Effettua una correzione di case altrimenti dbMeta.getPrimaryKeys()
   * non riesce ad individuare la tabella corretta.
   * @param metaSchemaName
   * @param metaTableName
   * @return i nomi di schema e tabella nel case corretto (conosciuto dal db)
   * @throws SQLException
   */
  protected Pair<String, String> correggiCase(String metaSchemaName, String metaTableName)
     throws SQLException
  {
    String rvs = null;

    if(metaSchemaName != null && !metaSchemaName.isEmpty())
    {
      try(ResultSet rsc = dbMeta.getSchemas())
      {
        while(rsc.next())
        {
          if(catalog != null && !catalog.equalsIgnoreCase(rsc.getString("TABLE_CATALOG")))
            continue;

          if(metaSchemaName.equalsIgnoreCase(rsc.getString("TABLE_SCHEM")))
          {
            rvs = rsc.getString("TABLE_SCHEM");
            break;
          }
        }
      }
    }

    try(ResultSet rSet = dbMeta.getTables(catalog, rvs, null, TABLES_FILTER))
    {
      while(rSet.next())
      {
        if(rSet.getString("TABLE_TYPE").equals("TABLE"))
        {
          String schema = rSet.getString("TABLE_SCHEM");
          String tableName = rSet.getString("TABLE_NAME");

          if(metaTableName.equalsIgnoreCase(tableName))
            return new Pair<>(schema, tableName);
        }
      }
    }

    return new Pair<>(metaSchemaName, metaTableName);
  }
}
