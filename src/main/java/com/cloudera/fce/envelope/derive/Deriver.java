package com.cloudera.fce.envelope.derive;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.sql.DataFrame;

import com.cloudera.fce.envelope.utils.PropertiesUtils;

/**
 * Derives the storage data model from the stream data model.
 */
public abstract class Deriver {
    
    /**
     * The properties of the deriver.
     */
    protected Properties props;
    
    public Deriver(Properties props) {
        this.props = props;
    }
    
    /**
     * Derive the storage records from the arriving stream records and the existing lookup records.
     * @param stream The collection of arriving records from the stream for the micro-batch.
     * @param lookups The collection of corresponding existing records for each lookup table. The
     * lookup tables are identified by their table name.
     * @return The collection of derived records that can be applied to the storage table.
     */
    public abstract DataFrame derive(DataFrame stream, Map<String, DataFrame> lookups) throws Exception;
    
    /**
     * Get the deriver for the flow.
     * @param props The properties of the flow.
     * @return The deriver.
     */
    public static Deriver deriverFor(Properties props) throws Exception {
        String deriverName = props.getProperty("deriver");
        Properties deriverProps = PropertiesUtils.prefixProperties(props, "deriver.");
        
        Deriver deriver = null;
        
        if (deriverName.equals("sql")) {
            deriver = new SQLDeriver(deriverProps);
        }
        else {
            Class<?> clazz = Class.forName(deriverName);
            Constructor<?> constructor = clazz.getConstructor(Properties.class);
            deriver = (Deriver)constructor.newInstance(deriverProps);
        }
        
        return deriver;
    }
    
}
