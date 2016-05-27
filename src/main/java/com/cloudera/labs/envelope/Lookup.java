package com.cloudera.labs.envelope;

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

import com.cloudera.labs.envelope.store.StorageSystems;
import com.cloudera.labs.envelope.store.StorageTable;
import com.cloudera.labs.envelope.utils.PropertiesUtils;
import com.cloudera.labs.envelope.utils.RecordUtils;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A lookup provides existing records to the deriver for the arriving records of the stream.
 */
@SuppressWarnings("serial")
public class Lookup implements Serializable {
    
    private Properties props;
    private String lookupName;
    
    public Lookup(Properties props, String lookupName) {
        this.props = props;
        this.lookupName = lookupName;
    }
    
    // Retrieve the corresponding lookup records for the provided arriving stream records
    public JavaRDD<GenericRecord> getLookupRecordsFor(JavaRDD<GenericRecord> arriving) {
        return arriving
            // Extract the filters (typically just the key) to send to the storage table
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
            // Get the unique list of filters so that we don't waste valuable time sending the
            // same request multiple times
            .distinct()
            // Send the filter to the storage and get back the zero-to-many records that match
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
        return lookupName;
    }
    
    // Get the mappings of stream field names to storage field names for when they are not the same
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
            Lookup lookup = new Lookup(lookupProps, lookupName);
            lookups.add(lookup);
        }
        
        return lookups;
    }
    
}
