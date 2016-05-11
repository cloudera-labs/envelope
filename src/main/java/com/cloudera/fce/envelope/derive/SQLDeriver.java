package com.cloudera.fce.envelope.derive;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.DataFrame;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

public class SQLDeriver extends Deriver {
    
    public SQLDeriver(Properties props) {
        super(props);
    }
    
    @Override
    public DataFrame derive(DataFrame stream, Map<String, DataFrame> lookups) throws Exception {
        String query;
        
        if (props.containsKey("query.literal")) {
            query = props.getProperty("query.literal");
        }
        else if (props.containsKey("query.file")) {
            query = hdfsFileAsString(props.getProperty("query.file"));
        }
        else {
            throw new RuntimeException("SQL deriver query not provided. Use 'query.literal' or 'query.file'.");
        }
        
        DataFrame derived = stream.sqlContext().sql(query);
        
        return derived;
    }
    
    private String hdfsFileAsString(String hdfsFile) throws Exception {
        String contents = null;
        
        FileSystem fs = FileSystem.get(new Configuration());
        InputStream stream = fs.open(new Path(hdfsFile));
        InputStreamReader reader = new InputStreamReader(stream, Charsets.UTF_8);
        contents = CharStreams.toString(reader);
        reader.close();
        stream.close();
        
        return contents;
    }
    
}
