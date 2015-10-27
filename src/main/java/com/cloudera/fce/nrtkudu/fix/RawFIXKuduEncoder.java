package com.cloudera.fce.nrtkudu.fix;

import java.util.Collections;
import java.util.List;

import com.cloudera.fce.nrtkudu.KuduEncoder;
import com.google.common.collect.Lists;

public class RawFIXKuduEncoder extends KuduEncoder {
    
    @Override
    protected String getTableName() {
        return "rawfix";
    }

    @Override
    protected List<String> getKeyFieldNames() {
        return Lists.newArrayList("message");
    }

    @Override
    protected List<String> getValueFieldNames() {                
        return Collections.emptyList();
    }

    @Override
    protected String getLastUpdatedFieldName() {
        return "last_updated";
    }
    
}
