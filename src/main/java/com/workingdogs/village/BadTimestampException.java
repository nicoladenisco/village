/*
 * Copyright (C) 2024 Nicola De Nisco
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

/**
 * Eccezione sollevata per errore di conversione.
 *
 * @author Nicola De Nisco
 */
public class BadTimestampException extends DataSetException
{
  public BadTimestampException(int columnNumber, Column column, Exception e)
  {
    super("Bad date value for " + columnNumber + "/"
       + (column == null ? "???" : column.name())
       + " - "
       + "Java Timestamp Objects cannot be earlier than 1/1/70", e);
  }
}
