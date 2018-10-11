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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.cloudera.labs.envelope.component.Component;
import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.validate.OptionalPathValidation;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.ValidationResult;
import com.cloudera.labs.envelope.validate.Validations;
import com.cloudera.labs.envelope.validate.Validator;
import com.cloudera.labs.envelope.validate.Validity;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.junit.Test;

public class TestSecurityUtils {

  private interface SecureInstantiator extends Component, InstantiatesComponents, UsesDelegationTokens {}
  private interface SecureComponent extends Component, UsesDelegationTokens {}
  private interface TopLevelInstantiates extends Component, InstantiatesComponents {}

  private interface SecurityValidator extends ProvidesValidations {}

  @Test
  public void testGetCredentialsFilePath() throws IOException {
    Config config = ConfigUtils.configFromResource("/security/security_manager_nocredsfile.conf");
    Contexts.initialize(config, Contexts.ExecutionMode.UNIT_TEST);
    String path = SecurityUtils.getTokenStoreFilePath(config, true);

    assertTrue(path.startsWith("/user"));
  }

  @Test
  public void testConfigIsValid() {
    Config config = ConfigUtils.configFromResource("/security/security_validation_test.conf");
    List<ValidationResult> results = Validator.validate(new SecurityValidator() {
      @Override
      public Validations getValidations() {
        return SecurityUtils.getValidations();
      }
    }, config);

    int count = 0;

    for (ValidationResult result : results) {
      if (result.getValidation() instanceof OptionalPathValidation) {
        ++count;
      }
      assertEquals(result.getValidity(), Validity.VALID);
    }

    assertEquals(5, count);
  }

  @Test
  public void testGetAllSecureComponents() throws Exception {
    TopLevelInstantiates topLevel = new TopLevelInstantiates() {
      @Override
      public Set<InstantiatedComponent> getComponents(Config config, boolean configure)
          throws Exception {
        SecureInstantiator secondLevel = new SecureInstantiator() {

          @Override
          public TokenProvider getTokenProvider() {
            return new TestTokenProvider();
          }

          @Override
          public Set<InstantiatedComponent> getComponents(Config config, boolean configure)
              throws Exception {

            SecureComponent thirdLevelA = new SecureComponent() {
              @Override
              public TokenProvider getTokenProvider() {
                return new TestTokenProvider();
              }
            };

            Component thirdLevelB = new Component() {};

            return Sets.newHashSet(new InstantiatedComponent(thirdLevelA, ConfigFactory.empty(), "tlA"),
                new InstantiatedComponent(thirdLevelB, ConfigFactory.empty(), "tlB"));
          }
        };

        return Collections.singleton(new InstantiatedComponent(secondLevel, ConfigFactory.empty(), "second"));
      }
    };

    Set<InstantiatedComponent> secureComponents = SecurityUtils.getAllSecureComponents(new InstantiatedComponent(topLevel, ConfigFactory.empty(), "top"));
    assertEquals(2, secureComponents.size());
  }

}
