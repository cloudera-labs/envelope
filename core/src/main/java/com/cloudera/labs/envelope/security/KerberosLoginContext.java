/*
 * Copyright (c) 2015-2019, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.labs.envelope.security;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

public class KerberosLoginContext {

  private LoginContext loginContext;
  private long lastLoggedIn = -1;

  public KerberosLoginContext(LoginContext loginContext) {
    this.loginContext = loginContext;
  }

  public LoginContext getLoginContext() {
    return loginContext;
  }

  public Subject getSubject() {
    return loginContext.getSubject();
  }

  public long getLastLoggedIn() {
    return lastLoggedIn;
  }

  public void recordLoginTime() {
    lastLoggedIn = System.currentTimeMillis();
  }

}
