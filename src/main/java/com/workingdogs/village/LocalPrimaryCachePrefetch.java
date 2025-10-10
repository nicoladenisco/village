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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.commonlib5.utils.StringOper;

/**
 * Cache delle chiavi primarie.
 * Viene utilizzata in Schema per stabilire se una colonna è chiave primaria.
 * Questa versione effettua un prefetch di tutte le tabelle e chiavi primarie
 * al primo utilizzo.
 * La ricerca di schema, tabelle e colonne è case insensitive.
 * Il popolamento è assicurato prelevando i dati direttamente dal db.
 *
 * <p>
 * DEPRECATA: su db come Oracle potrebbe caricare in memoria tutte le
 * tabelle del database, non solo quelle della nostra applicazione.
 * Usare LocalPrimaryCache che ha un approccio progressivo: una tabella per volta.
 * </p>
 *
 * @author Nicola De Nisco
 */
@Deprecated
public class LocalPrimaryCachePrefetch
{
  private final String catalog, connURL;
  private final DatabaseMetaData dbMeta;
  private static final TreeMap<String, TreeMap<String, Integer>> pkCache = new TreeMap<>(StringOper::compareIgnoreCase);
  private static final ArrayList<String> tableNames = new ArrayList<>();
  private static boolean initalized = false;
  private static final Object semaforo = new Object();

  public LocalPrimaryCachePrefetch(String catalog, DatabaseMetaData dbMeta)
     throws SQLException
  {
    this.dbMeta = dbMeta;
    this.catalog = catalog;
    this.connURL = dbMeta.getURL();

    if(!initalized)
    {
      synchronized(semaforo)
      {
        if(!initalized)
          prefetchData();

        initalized = true;
      }
    }
  }

  protected void prefetchData()
     throws SQLException
  {
    try(ResultSet rSet = dbMeta.getTables(catalog, null, null, TABLES_FILTER))
    {
      while(rSet.next())
      {
        if(rSet.getString("TABLE_TYPE").equals("TABLE"))
        {
          String schema = rSet.getString("TABLE_SCHEM");
          String tableName = rSet.getString("TABLE_NAME");
          tableNames.add(schema + "." + tableName);

          TreeMap<String, Integer> tablepks = new TreeMap<>();
          String key = makeKey(schema, tableName);

          if(dbMeta.getClass().getName().contains("Jtds"))
          {
            jtdsDriver(schema, tableName, tablepks);
          }
          else
          {
            allDriver(schema, tableName, tablepks);
          }

          pkCache.put(key, tablepks);
        }
      }
    }
  }

  /**
   * Cerca la colonna nella chiave primaria della tabella.
   * Accetta il formato metaSchemaName="" e metaTableName="SCHEMA.TABELLA"
   * @param metaSchemaName nome dello schema
   * @param metaTableName nome della tabella
   * @param metaColumnName nome della colonna
   * @return indice nella chiave priamaria; 0=non compare nella chiave primaria
   * @throws SQLException
   */
  public int findInPrimary(String metaSchemaName, String metaTableName, String metaColumnName)
     throws SQLException
  {
    Map<String, Integer> tablepks = getPrimaryKeyInfo(metaSchemaName, metaTableName);

    // tabella senza chiave primaria
    if(tablepks == null)
      return 0;

    return tablepks.getOrDefault(metaColumnName, 0);
  }

  /**
   * Ritorna il pacchetto di informazioni chiave primaria per una tabella.
   * Accetta il formato metaSchemaName="" e metaTableName="SCHEMA.TABELLA"
   * @param metaSchemaName nome dello schema
   * @param metaTableName nome della tabella
   * @return una mappa colonna/indice nella chiave primaria o null se tabella senza chiave primaria
   */
  public Map<String, Integer> getPrimaryKeyInfo(String metaSchemaName, String metaTableName)
  {
    int pos;
    if(metaSchemaName.isEmpty() && (pos = metaTableName.indexOf('.')) != -1)
    {
      // workaround nel caso metaTableName è nella forma SCHEMA.TABELLA
      metaSchemaName = metaTableName.substring(0, pos);
      metaTableName = metaTableName.substring(pos + 1);
    }

    String key = makeKey(metaSchemaName, metaTableName);
    TreeMap<String, Integer> tablepks = pkCache.get(key);
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

  public List<String> getTableNames()
  {
    return tableNames;
  }
}
