/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudera.labs.envelope.security;

import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.typesafe.config.Config;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.UnrecoverableKeyException;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;

public class TestCredentialProvider {

    private static final String JDBC_PROPERTIES_JCEKS_TYPE_CONF_FILE = "/CredentialProviderTest/jceks_type_file.properties";
    private static final String JDBC_PROPERTIES_LOCALJCEKS_TYPE_CONF = "/CredentialProviderTest/localjceks_type.properties";
    private static final String JDBC_PROPERTIES_PASSWORD_TYPE_CONF = "/CredentialProviderTest/password_type.properties";
    private static final String JDBC_PROPERTIES_FULL_SAMPLE_CREDENTIAL1_CONF = "/CredentialProviderTest/full_credential1.properties";
    private static final String JDBC_PROPERTIES_FULL_SAMPLE_CREDENTIAL2_CONF = "/CredentialProviderTest/full_credential2.properties";

    private static final String RESOURCE_LOCALJCEKS_PATH = "/CredentialProviderTest/testcreds.localjceks";
    private static final String RESOURCE_JCEKS_PATH = "/CredentialProviderTest/testcreds.jceks";
    private static final String CONF_LOCALJCEKS_PATH = "/tmp/testcreds.localjceks";
    private static final String CONF_JCEKS_PATH = "/tmp/testcreds.jceks";
    private static final String KEYSTORE1_PASS_FILE_PATH = "/CredentialProviderTest/none1.txt";
    private static final String TMP_KEYSTORE1_PASS_FILE = "/tmp/none1.txt";
    private static final String KEYSTORE2_PASS_FILE_PATH = "/CredentialProviderTest/none2.txt";
    private static final String TMP_KEYSTORE2_PASS_FILE = "/tmp/none2.txt";

    private static final String EXPECTED_PASSWORD_CONSTANT = "jcekspassword";

    @Before
    public void setup() throws IOException {

        Files.copy(TestCredentialProvider.class.getResourceAsStream(RESOURCE_LOCALJCEKS_PATH),
            new File(CONF_LOCALJCEKS_PATH).toPath(),
            StandardCopyOption.REPLACE_EXISTING);
        Files.copy(TestCredentialProvider.class.getResourceAsStream(RESOURCE_JCEKS_PATH),
            new File(CONF_JCEKS_PATH).toPath(),
            StandardCopyOption.REPLACE_EXISTING);
        /*Files.copy(TestCredentialProvider.class.getResourceAsStream(KEYSTORE1_PASS_FILE_PATH),
            new File(TMP_KEYSTORE1_PASS_FILE).toPath(),
            StandardCopyOption.REPLACE_EXISTING);
        Files.copy(TestCredentialProvider.class.getResourceAsStream(KEYSTORE2_PASS_FILE_PATH),
            new File(TMP_KEYSTORE2_PASS_FILE).toPath(),
            StandardCopyOption.REPLACE_EXISTING);*/

    }

    /**
     * Test of testGetPasswordCredential_Provider method, of class
     * OldCredentialProviderAdapter.
     *
     * @param strPath
     * @return
     * @throws java.lang.Exception
     */
    public String testGetPasswordCredential(String strPath) throws Exception {
        Config config = ConfigUtils.configFromPath(CredentialProvider.class.getResource(strPath).getPath());
        CredentialProvider instance = CredentialProviderFactory.create(config);
        return instance.getPassword();
    }

    @Test
    public void testGetPasswordFrom_jceks_type_file() throws Exception {
        assertEquals(EXPECTED_PASSWORD_CONSTANT, testGetPasswordCredential(
            JDBC_PROPERTIES_JCEKS_TYPE_CONF_FILE));
    }

    @Test
    public void testGetPasswordFrom_localjceks_type() throws Exception {
        assertEquals(EXPECTED_PASSWORD_CONSTANT, testGetPasswordCredential(
            JDBC_PROPERTIES_LOCALJCEKS_TYPE_CONF));
    }

    @Test
    public void testGetPasswordFrom_password_type() throws Exception {
        assertEquals(EXPECTED_PASSWORD_CONSTANT, testGetPasswordCredential(
            JDBC_PROPERTIES_PASSWORD_TYPE_CONF));
    }

    @Test
    public void testGetPasswordFrom_full_credential1() throws Exception {
        assertEquals(EXPECTED_PASSWORD_CONSTANT, testGetPasswordCredential(
            JDBC_PROPERTIES_FULL_SAMPLE_CREDENTIAL1_CONF));
    }
    
    @Test(expected=IOException.class)
    public void testGetPasswordFrom_full_credential2() throws Exception {
        assertFalse(EXPECTED_PASSWORD_CONSTANT.equals(testGetPasswordCredential(
            JDBC_PROPERTIES_FULL_SAMPLE_CREDENTIAL2_CONF)));
    }

    /**
     * This Method deletes all credentials from /tmp location
     */
    @After
    public void teardown() {
        new File(CONF_LOCALJCEKS_PATH).delete();
        new File(CONF_JCEKS_PATH).delete();
        /*new File(TMP_KEYSTORE1_PASS_FILE).delete();
        new File(TMP_KEYSTORE2_PASS_FILE).delete();*/

    }
}
