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

package com.cloudera.labs.envelope.validate;

import com.cloudera.labs.envelope.component.Component;
import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import com.typesafe.config.ConfigValueType;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static com.cloudera.labs.envelope.validate.ValidationAssert.assertValidationFailures;

public class TestValidator {

  // Helps us build anonymous classes that implement multiple interfaces
  private interface ValidatableInstantiator extends ProvidesValidations, InstantiatesComponents {}
  private interface ValidatableComponent extends ProvidesValidations, Component {}

  @Test
  public void testValid() {
    ProvidesValidations validee = new ProvidesValidations() {
      @Override
      public Validations getValidations() {
        return Validations.builder().mandatoryPath("hello", ConfigValueType.STRING).build();
      }
    };

    Properties configProps = new Properties();
    configProps.setProperty("hello", "world");
    Config config = ConfigFactory.parseProperties(configProps);

    assertNoValidationFailures(validee, config);
  }

  @Test
  public void testInvalid() {
    ProvidesValidations validee = new ProvidesValidations() {
      @Override
      public Validations getValidations() {
        return Validations.builder().mandatoryPath("hello", ConfigValueType.STRING).build();
      }
    };

    assertValidationFailures(validee, ConfigFactory.empty());
  }

  @Test
  public void testValidWithinInstantiation() {
    Properties innerConfigProps = new Properties();
    innerConfigProps.setProperty("hello", "world");
    final Config innerConfig = ConfigFactory.parseProperties(innerConfigProps);

    ProvidesValidations validee = new ValidatableInstantiator() {
      @Override
      public Validations getValidations() {
        return Validations.builder().build();
      }
      @Override
      public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
        return Sets.newHashSet(new InstantiatedComponent(new ValidatableComponent() {
          @Override
          public Validations getValidations() {
            return Validations.builder().mandatoryPath("hello").build();
          }
        }, innerConfig, "Inner"));
      }
    };

    assertNoValidationFailures(validee, ConfigFactory.empty());
  }

  @Test
  public void testInvalidWithinInstantiation() {
    Properties innerConfigProps = new Properties();
    final Config innerConfig = ConfigFactory.parseProperties(innerConfigProps);

    ProvidesValidations validee = new ValidatableInstantiator() {
      @Override
      public Validations getValidations() {
        return Validations.builder().build();
      }
      @Override
      public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
        return Sets.newHashSet(new InstantiatedComponent(new ValidatableComponent() {
          @Override
          public Validations getValidations() {
            return Validations.builder().mandatoryPath("hello").build();
          }
        }, innerConfig, "Inner"));
      }
    };

    assertValidationFailures(validee, ConfigFactory.empty());
  }

  @Test
  public void testTypeNotSpecifiedAsValidationIsForgiven() {
    ProvidesValidations validee = new ProvidesValidations() {
      @Override
      public Validations getValidations() {
        return Validations.builder().build();
      }
    };

    Properties configProps = new Properties();
    configProps.setProperty("type", "world");
    Config config = ConfigFactory.parseProperties(configProps);

    assertNoValidationFailures(validee, config);
  }

  @Test
  public void testValidationThrowsException() {
    ProvidesValidations validee = new ProvidesValidations() {
      @Override
      public Validations getValidations() {
        return Validations.builder()
            .add(new Validation() {
              @Override
              public ValidationResult validate(Config config) {
                throw new RuntimeException("oops");
              }
              @Override
              public Set<String> getKnownPaths() {
                return Sets.newHashSet();
              }
            })
            .build();
      }
    };

    assertNoValidationFailures(validee, ConfigFactory.empty());
  }

  @Test
  public void testInstantiationThrowsException() {
    ProvidesValidations validee = new ValidatableInstantiator() {
      @Override
      public Validations getValidations() {
        return Validations.builder().build();
      }
      @Override
      public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
        throw new RuntimeException("oops");
      }
    };

    assertValidationFailures(validee, ConfigFactory.empty());
  }

  @Test
  public void testUnrecognizedPaths() {
    ProvidesValidations validee = new ProvidesValidations() {
      @Override
      public Validations getValidations() {
        return Validations.builder().build();
      }
    };

    Properties configProps = new Properties();
    configProps.setProperty("hello", "world");
    Config config = ConfigFactory.parseProperties(configProps);

    assertValidationFailures(validee, config);
  }

  @Test
  public void testAllowUnrecognizedPaths() {
    ProvidesValidations validee = new ProvidesValidations() {
      @Override
      public Validations getValidations() {
        return Validations.builder().allowUnrecognizedPaths().build();
      }
    };

    Properties configProps = new Properties();
    configProps.setProperty("hello", "world");
    Config config = ConfigFactory.parseProperties(configProps);

    assertNoValidationFailures(validee, config);
  }

  @Test
  public void testHandlesOwnValidation() {
    ProvidesValidations validee = new ProvidesValidations() {
      @Override
      public Validations getValidations() {
        return Validations.builder().handlesOwnValidationPath("hello").build();
      }
    };

    Properties configProps = new Properties();
    configProps.setProperty("hello", "world");
    Config config = ConfigFactory.parseProperties(configProps);

    assertNoValidationFailures(validee, config);
  }

  @Test
  public void testEmptyValue() {
    ProvidesValidations validee = new ProvidesValidations() {
      @Override
      public Validations getValidations() {
        return Validations.builder().mandatoryPath("hello").build();
      }
    };

    Properties configProps = new Properties();
    configProps.setProperty("hello", "");
    Config config = ConfigFactory.parseProperties(configProps);

    assertValidationFailures(validee, config);

    config = config.withValue("hello", ConfigValueFactory.fromAnyRef(null));
    assertValidationFailures(validee, config);
  }

  @Test
  public void testAllowEmptyValue() {
    ProvidesValidations validee = new ProvidesValidations() {
      @Override
      public Validations getValidations() {
        return Validations.builder().mandatoryPath("hello").allowEmptyValue("hello").build();
      }
    };

    Properties configProps = new Properties();
    configProps.setProperty("hello", "");
    Config config = ConfigFactory.parseProperties(configProps);
    assertNoValidationFailures(validee, config);

    config = config.withValue("hello", ConfigValueFactory.fromIterable(
        Lists.newArrayList("")));
    assertNoValidationFailures(validee, config);
  }

  @Test
  public void testDisabled() {
    ProvidesValidations validee = new ProvidesValidations() {
      @Override
      public Validations getValidations() {
        return Validations.builder().mandatoryPath("hello").build();
      }
    };

    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(Validator.CONFIGURATION_VALIDATION_ENABLED_PROPERTY, false);
    Config config = ConfigFactory.parseMap(configMap);

    assertNoValidationFailures(validee, config);
  }

  @Test
  public void testDisabledWithinInstantiation() {
    Map<String, Object> innerConfigMap = Maps.newHashMap();
    innerConfigMap.put(Validator.CONFIGURATION_VALIDATION_ENABLED_PROPERTY, false);
    final Config innerConfig = ConfigFactory.parseMap(innerConfigMap);

    ProvidesValidations validee = new ValidatableInstantiator() {
      @Override
      public Validations getValidations() {
        return Validations.builder().build();
      }
      @Override
      public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
        return Sets.newHashSet(new InstantiatedComponent(new ValidatableComponent() {
          @Override
          public Validations getValidations() {
            return Validations.builder().mandatoryPath("hello").build();
          }
        }, innerConfig, "Inner"));
      }
    };

    assertNoValidationFailures(validee, ConfigFactory.empty());
  }

}
