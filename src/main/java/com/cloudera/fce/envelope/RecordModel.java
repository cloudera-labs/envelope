package com.cloudera.fce.envelope;

import java.io.Serializable;
import java.util.List;

@SuppressWarnings("serial")
public class RecordModel implements Serializable {

    private List<String> keyFieldNames;
    private String timestampFieldName;
    private List<String> valueFieldNames;
    private String lastUpdatedFieldName;
    private String effectiveFromFieldName;
    private String effectiveToFieldName;
    private String currentFlagFieldName;
    
    public List<String> getKeyFieldNames() {
        return keyFieldNames;
    }
    public void setKeyFieldNames(List<String> keyFieldNames) {
        this.keyFieldNames = keyFieldNames;
    }
    
    public String getTimestampFieldName() {
        return timestampFieldName;
    }
    public void setTimestampFieldName(String timestampFieldName) {
        this.timestampFieldName = timestampFieldName;
    }
    
    public List<String> getValueFieldNames() {
        return valueFieldNames;
    }
    public void setValueFieldNames(List<String> valueFieldNames) {
        this.valueFieldNames = valueFieldNames;
    }
    
    public boolean hasLastUpdatedField() {
        return getLastUpdatedFieldName() != null;
    }
    public String getLastUpdatedFieldName() {
        return lastUpdatedFieldName;
    }
    public void setLastUpdatedFieldName(String lastUpdatedFieldName) {
        this.lastUpdatedFieldName = lastUpdatedFieldName;
    }
    
    public boolean hasEffectiveFromField() {
        return getEffectiveFromFieldName() != null;
    }
    public String getEffectiveFromFieldName() {
        return effectiveFromFieldName;
    }
    public void setEffectiveFromFieldName(String effectiveFromFieldName) {
        this.effectiveFromFieldName = effectiveFromFieldName;
    }
    
    public boolean hasEffectiveToField() {
        return getEffectiveToFieldName() != null;
    }
    public String getEffectiveToFieldName() {
        return effectiveToFieldName;
    }
    public void setEffectiveToFieldName(String effectiveToFieldName) {
        this.effectiveToFieldName = effectiveToFieldName;
    }
    
    public boolean hasCurrentFlagField() {
        return getCurrentFlagFieldName() != null;
    }
    public String getCurrentFlagFieldName() {
        return currentFlagFieldName;
    }
    public void setCurrentFlagFieldName(String currentFlagFieldName) {
        this.currentFlagFieldName = currentFlagFieldName;
    }
    
}
