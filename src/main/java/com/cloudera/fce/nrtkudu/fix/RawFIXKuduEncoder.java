package com.cloudera.fce.nrtkudu.fix;

import java.util.List;

import com.cloudera.fce.nrtkudu.KuduEncoder;
import com.google.common.collect.Lists;

public class RawFIXKuduEncoder extends KuduEncoder {
    
    @Override
    protected String getTableName() {
        return "fix_raw";
    }
    
    @Override
    protected List<String> getKeyFieldNames() {
        return Lists.newArrayList("message");
    }
    
    @Override
    protected String getTimestampFieldName() {
        return "last_updated";
    }
    
    @Override
    protected String getLastUpdatedFieldName() {
        return "last_updated";
    }
    
    @Override
    protected boolean doesTrackHistory() {
        return false;
    }
    
}