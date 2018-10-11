/*
 * Copyright (c) 2015-2018, Cloudera, Inc. All Rights Reserved.
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.token.Token;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestTokenStore {

  @After
  public void cleanup() throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    fs.delete(new Path("tokenfile.token"), false);
  }

  @Test
  public void testAddGetToken() {
    Token token = SecurityUtils.createToken(new byte[] {0x00, 0x01, 0x02, 0x03}, 0);

    TokenStore wrapper = new TokenStore();
    wrapper.addToken("test-token", token);

    assertEquals(1, wrapper.getTokenAliases().size());
    assertEquals("test-token", wrapper.getTokenAliases().iterator().next());
    assertEquals(0, ByteBuffer.wrap(wrapper.getToken("test-token").getIdentifier()).getInt());
    assertArrayEquals(new byte[] {0x00, 0x01, 0x02, 0x03}, wrapper.getToken("test-token").getPassword());
  }

  @Test
  public void testWriteReadToken() {
    Token token = SecurityUtils.createToken(new byte[] {0x00, 0x01, 0x02, 0x03}, 0);

    TokenStore writer = new TokenStore();
    writer.addToken("test-token", token);

    try {
      writer.write("tokenfile.token", new Configuration());
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }

    TokenStore reader = new TokenStore();
    try {
      reader.read("tokenfile.token", new Configuration());
      assertEquals(1, reader.getTokenAliases().size());
      assertEquals("test-token", reader.getTokenAliases().iterator().next());
      assertEquals(0, ByteBuffer.wrap(reader.getToken("test-token").getIdentifier()).getInt());
      assertArrayEquals(new byte[] {0x00, 0x01, 0x02, 0x03}, reader.getToken("test-token").getPassword());
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
  }

}
