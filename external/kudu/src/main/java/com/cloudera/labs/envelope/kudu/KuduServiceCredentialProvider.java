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
package com.cloudera.labs.envelope.kudu;

import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.security.ServiceCredentialProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.util.Set;
import java.util.regex.Pattern;

public class KuduServiceCredentialProvider implements ServiceCredentialProvider {
  
  public static final String CREDENTIAL_ALIAS_PREFIX = "envelope.kudu.";
  public static final String ENVELOPE_CONFIGURATION_CONF = "envelope.configuration";
  public static final String KUDU_MASTER_ADDRESSES_CONF = "spark.kudu.master.addresses";
  
  private static Logger LOG = LoggerFactory.getLogger(KuduServiceCredentialProvider.class);

  @Override
  public boolean credentialsRequired(SparkConf sparkConf, Configuration conf) {
    return UserGroupInformation.isSecurityEnabled();
  }

  @Override
  public Option<Object> obtainCredentials(Configuration conf, SparkConf sparkConf, Credentials creds) {
    Set<String> masterAddresseses = getAllKuduMasterAddresses(sparkConf);
    
    for (String masterAddresses : masterAddresseses) {
      byte[] token;
      try {
        KuduClient client = new KuduClient.KuduClientBuilder(masterAddresses).build();
        token = client.exportAuthenticationCredentials();
        client.close();
      }
      catch (KuduException e) {
        throw new RuntimeException(e);
      }
      
      Text credAlias = new Text(CREDENTIAL_ALIAS_PREFIX + masterAddresses);
      Token<?> credToken = new Token<>(null, token, new Text("auth"), new Text("envelope-kudu"));
      creds.addToken(credAlias, credToken);
      
      LOG.info("Kudu authentication credentials obtained for master addresses: " + masterAddresses);
    }
    
    return Option.empty();
  }

  @Override
  public String serviceName() {
    return "envelope-kudu";
  }
  
  private Set<String> getAllKuduMasterAddresses(SparkConf sparkConf) {
    // This configuration will only be available in client mode, but is automatically populated by Envelope.
    if (sparkConf.contains(ENVELOPE_CONFIGURATION_CONF)) {
      String envelopeString = sparkConf.get(ENVELOPE_CONFIGURATION_CONF);
      Config envelopeConfig = ConfigFactory.parseString(envelopeString);
      
      Set<String> masterAddresseses = Sets.newHashSet();
      
      Set<String> stepNames = envelopeConfig.getObject("steps").keySet();
      for (String stepName : stepNames) {
        Config stepConfig = envelopeConfig.getConfig("steps").getConfig(stepName);
        
        if (stepConfig.hasPath("output")) {
          Config outputConfig = stepConfig.getConfig("output");
          
          if (outputConfig.getString("type").equals("kudu")) {
            masterAddresseses.add(outputConfig.getString("connection"));
          }
        }
      }
      
      return masterAddresseses;
    }
    // This configuration is available in all modes, but has to be manually provided by the user.
    else if (sparkConf.contains(KUDU_MASTER_ADDRESSES_CONF)) {
      String masterAddressesConf = sparkConf.get(KUDU_MASTER_ADDRESSES_CONF);
      
      return Sets.newHashSet(masterAddressesConf.split(Pattern.quote(";")));
    }
    // If neither of the above configurations are available then we assume the job is not using the
    // Kudu Java API with security enabled, so we return no Kudu master addresses.
    else {
      return Sets.newHashSet();
    }
  }
  
}