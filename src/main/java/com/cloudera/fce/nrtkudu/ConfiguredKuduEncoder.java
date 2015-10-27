package com.cloudera.fce.nrtkudu;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

// Proof of concept for an encoder that can run entirely from configuration.
// This could be useful when there are no derived fields.

public class ConfiguredKuduEncoder extends KuduEncoder {
    
    Properties props;

    public ConfiguredKuduEncoder(Properties props) {
        this.props = props;
    }
    
    @Override
    protected String getTableName() {
        return props.getProperty("table.name");
    }

    @Override
    protected List<String> getKeyFieldNames() {
        return Arrays.asList(props.getProperty("fields.key").split(","));
    }

    @Override
    protected List<String> getValueFieldNames() {                
        return Arrays.asList(props.getProperty("fields.values").split(","));
    }

    @Override
    protected String getLastUpdatedFieldName() {
        return props.getProperty("fields.lastupdated");
    }

}
