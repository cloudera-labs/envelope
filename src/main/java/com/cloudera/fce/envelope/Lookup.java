package com.cloudera.fce.envelope;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.kududb.client.shaded.com.google.common.collect.Lists;
import org.kududb.client.shaded.com.google.common.collect.Maps;

import com.cloudera.fce.envelope.storage.StorageSystems;
import com.cloudera.fce.envelope.storage.StorageTable;
import com.cloudera.fce.envelope.utils.PropertiesUtils;
import com.cloudera.fce.envelope.utils.RecordUtils;

@SuppressWarnings("serial")
public class Lookup implements Serializable {
    
    private Properties props;
    
    public Lookup(Properties props) {
        this.props = props;
    }
    
    public JavaRDD<GenericRecord> getLookupRecordsFor(JavaRDD<GenericRecord> arriving) {
        return arriving
        .flatMap(new FlatMapFunction<GenericRecord, GenericRecord>() {
            boolean initialized = false;
            transient StorageTable storageTable;
            transient Schema lookupFilterSchema;
            transient Map<String, String> fieldMappings;
            
            @Override
            public Iterable<GenericRecord> call(GenericRecord arriving) throws Exception {
                if (!initialized) {
                    storageTable = StorageSystems.tableFor(props);
                    fieldMappings = extractFieldMappings(props.getProperty("stream.field.mapping"));
                    List<String> keyFieldNames = PropertiesUtils.propertyAsList(props, "storage.table.columns.key");
                    lookupFilterSchema = RecordUtils.subsetSchema(arriving.getSchema(), keyFieldNames, fieldMappings);
                    initialized = true;
                }
                
                GenericRecord lookupKey = RecordUtils.subsetRecord(arriving, lookupFilterSchema, fieldMappings);
                
                List<GenericRecord> existing = storageTable.getExistingForFilter(lookupKey);
                
                return existing;
            }
        })
        .distinct();
    }
    
    public Schema getLookupTableSchema() throws Exception {
        StorageTable lookupTable = StorageSystems.tableFor(props);
        
        return lookupTable.getSchema();
    }
    
    public String getLookupTableName() {
        return props.getProperty("storage.table.name");
    }
    
    private Map<String, String> extractFieldMappings(String fieldMappingProperty) {
        if (fieldMappingProperty == null) {
            return null;
        }
        
        Map<String, String> fieldMappings = Maps.newHashMap();
        
        String[] fieldMappingStrings = fieldMappingProperty.split(Pattern.quote(","));
        
        for (String fieldMappingString : fieldMappingStrings) {
            String[] components = fieldMappingString.split(Pattern.quote(":"));
            String originalFieldName = components[0];
            String newFieldName = components[1];
            
            fieldMappings.put(originalFieldName, newFieldName);
        }
        
        return fieldMappings;
    }
    
    public static List<Lookup> lookupsFor(Properties props) {
        List<Lookup> lookups = Lists.newArrayList();
        
        if (!props.containsKey("lookups")) {
            return lookups;
        }
        
        String[] lookupNames = props.getProperty("lookups").split(Pattern.quote(","));
        
        for (String lookupName : lookupNames) {
            Properties lookupProps = PropertiesUtils.prefixProperties(props, "lookup." + lookupName + ".");
            Lookup lookup = new Lookup(lookupProps);
            lookups.add(lookup);
        }
        
        return lookups;
    }
    
}
