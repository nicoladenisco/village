/*
 * Copyright (C) 2023 Nicola De Nisco
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

import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Classe di supporto per l'utilizzo del db Derby per le unit di test.
 *
 * @author Nicola De Nisco
 */
public class DerbyTestHelper
{
  public static final String driver = "org.apache.derby.jdbc.EmbeddedDriver";
  public static final String protocol = "jdbc:derby:";

  public Connection con;
  public Properties props = new Properties();

  public void init()
     throws Exception
  {
    // carica il driver e inizializza il motore db
    Class.forName(driver).newInstance();
    con = DriverManager.getConnection(protocol + "target/derbyDB;create=true", props);
  }

  public void shutdown()
  {
    try
    {
      // chiude tutti i database e il motore db
      DriverManager.getConnection("jdbc:derby:;shutdown=true");
    }
    catch(SQLException ex)
    {
      // ignorata
    }
  }

  public void buildDb1()
     throws Exception
  {
    try (InputStream ires = this.getClass().getResourceAsStream("/db1.sql"))
    {
      VillageUtils.executeSqlScript(con, new InputStreamReader(ires, "UTF-8"), true);
    }
  }

  public boolean existTable(String tableName)
  {
    try
    {
      String query
         = "SELECT TRUE "
         + "  FROM SYS.SYSTABLES "
         + " WHERE TABLENAME = ? AND TABLETYPE = 'T'"; // Leave TABLETYPE out if you don't care about it

      try (PreparedStatement ps = con.prepareStatement(query))
      {
        // il nome tabella deve essere maiuscolo
        ps.setString(1, "TRANSCODE");
        try (ResultSet rs = ps.executeQuery())
        {
          return (rs.next() && rs.getBoolean(1));
        }
      }
    }
    catch(Throwable t)
    {
      return false;
    }
  }
}
