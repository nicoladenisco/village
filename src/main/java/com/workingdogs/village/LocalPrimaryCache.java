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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
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
  private static final HashMap<String, Map<String, Integer>> pkCache = new HashMap<>(256);

  public LocalPrimaryCache(String catalog, DatabaseMetaData dbMeta)
     throws SQLException
  {
    this.dbMeta = dbMeta;
    this.catalog = catalog;
    this.connURL = dbMeta.getURL();
  }

  public int findInPrimary(String metaSchemaName, String metaTableName, String metaColumnName)
     throws SQLException
  {
    String key = connURL + "|" + StringOper.okStr(metaSchemaName, "NO_SCHEMA") + "|" + metaTableName;

    Map<String, Integer> tablepks = pkCache.get(key);

    if(tablepks == null)
    {
      tablepks = new HashMap<>();

      if(dbMeta.getClass().getName().contains("Jtds"))
      {
        jtdsDriver(metaSchemaName, metaTableName, tablepks);
      }
      else
      {
        allDriver(metaSchemaName, metaTableName, tablepks);
      }

      pkCache.put(key, tablepks);
    }

    return tablepks.getOrDefault(metaColumnName, 0);
  }

  protected void allDriver(String metaSchemaName, String metaTableName, Map<String, Integer> tablepks)
     throws SQLException
  {
    try (ResultSet dbPrimary = dbMeta.getPrimaryKeys(catalog, metaSchemaName, metaTableName))
    {
      while(dbPrimary.next())
      {
        String nomeColonna = dbPrimary.getString("COLUMN_NAME");
        int kinfo = dbPrimary.getInt("KEY_SEQ");
        tablepks.put(nomeColonna, kinfo);
      }
    }
  }

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

    try (ResultSet dbPrimary = dbMeta.getPrimaryKeys(sc, "dbo", metaTableName))
    {
      while(dbPrimary.next())
      {
        String nomeColonna = dbPrimary.getString("COLUMN_NAME");
        int kinfo = dbPrimary.getInt("KEY_SEQ");
        tablepks.put(nomeColonna, kinfo);
      }
    }
  }
}
