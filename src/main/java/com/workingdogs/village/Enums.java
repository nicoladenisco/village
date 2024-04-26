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

/**
 * A class for constants.
 *
 * @author <a href="mailto:jon@latchkey.com">Jon S. Stevens</a>
 * @version $Revision: 567 $
 */
public class Enums
{
    /**
     * Doesn't do anything
     */
    Enums()
    {
    }

    /** DataSet record that has been deleted but not removed from the DataSet */
    public static final int ZOMBIE = 1;

    /** A record marked for delete */
    public static final int DELETE = 2;

    /** A record marked for update */
    public static final int UPDATE = 3;

    /** A record marked for insert */
    public static final int INSERT = 4;

    /** trigger state before a delete is run */
    public static final int BEFOREDELETE = 5;

    /** trigger state after a delete is run */
    public static final int AFTERDELETE = 6;

    /** trigger state before a insert is run */
    public static final int BEFOREINSERT = 7;

    /** trigger state after a insert is run */
    public static final int AFTERINSERT = 8;

    /** trigger state before a update is run */
    public static final int BEFOREUPDATE = 9;

    /** trigger state after a update is run */
    public static final int AFTERUPDATE = 10;

    /** an unknown type */
    public static final int UNKNOWN = 11;

    /** an oracle type */
    public static final int ORACLE = 12;

    /** an sybase type */
    public static final int SYBASE = 13;

    /** an sqlserver type */
    public static final int SQLSERVER = 14;
}
