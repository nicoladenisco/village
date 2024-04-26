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
 * A DataSetException is thrown if there is an error.
 *
 * @author <a href="mailto:jon@latchkey.com">Jon S. Stevens</a>
 * @version $Revision: 568 $
 */
public class DataSetException
   extends Exception
{
  /** Serial Version UID */
  private static final long serialVersionUID = -1898644287113084556L;

  /**
   * Creates a new DataSetException object.
   */
  public DataSetException()
  {
    super();
  }

  /**
   * Creates a new DataSetException object.
   *
   * @param s
   */
  public DataSetException(String s)
  {
    super(s);
  }

  public DataSetException(String message, Throwable cause)
  {
    super(message, cause);
  }
}
