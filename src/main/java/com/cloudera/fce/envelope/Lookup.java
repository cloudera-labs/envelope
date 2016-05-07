package com.cloudera.fce.envelope;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import com.cloudera.fce.envelope.storage.StorageSystems;
import com.cloudera.fce.envelope.storage.StorageTable;
import com.cloudera.fce.envelope.utils.PropertiesUtils;
import com.cloudera.fce.envelope.utils.RecordUtils;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@SuppressWarnings("serial")
public class Lookup implements Serializable {
    
    private Properties props;
    
    public Lookup(Properties props) {
        this.props = props;
    }
    
    public JavaRDD<GenericRecord> getLookupRecordsFor(JavaRDD<GenericRecord> arriving) {
        return arriving
        .map(new Function<GenericRecord, GenericRecord>() {
            boolean initialized = false;
            transient Schema lookupFilterSchema;
            transient Map<String, String> fieldMappings;
            @Override
            public GenericRecord call(GenericRecord arrived) throws Exception {
                if (!initialized) {
                    fieldMappings = extractFieldMappings(props.getProperty("stream.field.mapping"));
                    List<String> keyFieldNames = PropertiesUtils.propertyAsList(props, "storage.table.columns.key");
                    lookupFilterSchema = RecordUtils.subsetSchema(arrived.getSchema(), keyFieldNames, fieldMappings);
                    initialized = true;
                }
                GenericRecord lookupFilter = RecordUtils.subsetRecord(arrived, lookupFilterSchema, fieldMappings);
                return lookupFilter;
            }
        })
        .distinct()
        .flatMap(new FlatMapFunction<GenericRecord, GenericRecord>() {
            transient StorageTable storageTable;
            @Override
            public Iterable<GenericRecord> call(GenericRecord lookupKey) throws Exception {
                if (storageTable == null) {
                    storageTable = StorageSystems.tableFor(props);
                }
                List<GenericRecord> existing = storageTable.getExistingForFilter(lookupKey);
                return existing;
            }
        });
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
    
    public static Set<Lookup> lookupsFor(Properties props) {
        Set<Lookup> lookups = Sets.newHashSet();
        
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
