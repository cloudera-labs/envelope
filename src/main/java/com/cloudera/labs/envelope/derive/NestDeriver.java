package com.cloudera.labs.envelope.derive;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import scala.Tuple2;

public class NestDeriver implements Deriver {
    
    public static final String NEST_INTO_CONFIG_NAME = "nest.into";
    public static final String NEST_FROM_CONFIG_NAME = "nest.from";
    public static final String KEY_FIELD_NAMES_CONFIG_NAME = "key.field.names";
    public static final String NESTED_FIELD_NAME_CONFIG_NAME = "nested.field.name";

    private Config config;
    
    @Override
    public void configure(Config config) {
        this.config = config;
        
        for (String configName : Lists.newArrayList(NESTED_FIELD_NAME_CONFIG_NAME, NEST_FROM_CONFIG_NAME,
                KEY_FIELD_NAMES_CONFIG_NAME, NESTED_FIELD_NAME_CONFIG_NAME))
        {
            if (!config.hasPath(configName)) {
                throw new RuntimeException("Nest deriver requires '" + configName + "' property");
            }
        }
    }

    @Override
    public DataFrame derive(Map<String, DataFrame> dependencies) throws Exception {
        String intoDependency = config.getString(NEST_INTO_CONFIG_NAME);
        if (!dependencies.containsKey(intoDependency)) {
            throw new RuntimeException("Nest deriver points to non-existent nest-into dependency");
        }
        DataFrame into = dependencies.get(intoDependency);
        
        String fromDependency = config.getString(NEST_FROM_CONFIG_NAME);
        if (!dependencies.containsKey(fromDependency)) {
            throw new RuntimeException("Nest deriver points to non-existent nest-from dependency");
        }
        DataFrame from = dependencies.get(fromDependency);
        
        List<String> keyFieldNames = config.getStringList(KEY_FIELD_NAMES_CONFIG_NAME);
        String nestedFieldName = config.getString(NESTED_FIELD_NAME_CONFIG_NAME);
        
        ExtractFieldsFunction extractFieldsFunction = new ExtractFieldsFunction(keyFieldNames);
        JavaPairRDD<List<Object>, Row> keyedIntoRDD = into.javaRDD().keyBy(extractFieldsFunction);
        JavaPairRDD<List<Object>, Row> keyedFromRDD = from.javaRDD().keyBy(extractFieldsFunction);

        NestFunction nestFunction = new NestFunction();
        JavaRDD<Row> nestedRDD = keyedIntoRDD.cogroup(keyedFromRDD).values().map(nestFunction);
        
        StructType nestedSchema = into.schema().add(nestedFieldName, DataTypes.createArrayType(from.schema()));
        
        DataFrame nested = into.sqlContext().createDataFrame(nestedRDD, nestedSchema);
        
        return nested;
    }
    
    @SuppressWarnings("serial")
    private static class ExtractFieldsFunction implements Function<Row, List<Object>> {
        private List<String> fieldNames;
        
        public ExtractFieldsFunction(List<String> fieldNames) {
            this.fieldNames = fieldNames;
        }
        
        @Override
        public List<Object> call(Row row) throws Exception {
            List<Object> values = new ArrayList<>();
            
            for (String fieldName : fieldNames) {
                values.add(row.get(row.fieldIndex(fieldName)));
            }
            
            return values;
        }
    }
    
    @SuppressWarnings("serial")
    private static class NestFunction implements Function<Tuple2<Iterable<Row>, Iterable<Row>>, Row> {
        @Override
        public Row call(Tuple2<Iterable<Row>, Iterable<Row>> cogrouped) throws Exception {
            // There should only be one 'into' record per key
            Row intoRow = cogrouped._1().iterator().next();
            Iterable<Row> fromRows = cogrouped._2();
            int intoRowNumFields = intoRow.size();
            
            Object[] nestedValues = new Object[intoRowNumFields + 1];
            for (int i = 0; i < intoRowNumFields; i++) {
                nestedValues[i] = intoRow.get(i);
            }
            nestedValues[intoRowNumFields] = fromRows;
            
            Row nested = RowFactory.create(nestedValues);
            
            return nested;
        }
    }

}
