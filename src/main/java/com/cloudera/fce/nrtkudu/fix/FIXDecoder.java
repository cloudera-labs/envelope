package com.cloudera.fce.nrtkudu.fix;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.cloudera.fce.nrtkudu.Decoder;
import com.google.common.collect.Lists;

// Example decoder for FIX messages for demonstration purposes.
// Could be extended to a more useful scope of fields.

@SuppressWarnings("serial")
public class FIXDecoder extends Decoder {
    
    Schema schema = null;

    @Override
    public List<GenericRecord> decode(Iterable<String> inputs) {
        List<GenericRecord> records = Lists.newArrayList();
        
        for (String input : inputs) {
            GenericRecord rec = new GenericData.Record(getSchema());
            
            // FIX message key-value-pairs are separated with a ASCII 1 delimiter.
            String[] kvps = input.split(String.valueOf((char) 1));
            Map<Integer, String> tags = new HashMap<>();
            for (String kvp : kvps) {
                // FIX message keys and values are separated with an equals sign delimiter.
                String[] components = kvp.split("=");
                tags.put(Integer.valueOf(components[0]), components[1]);
            }
            
            rec.put("MsgType", tags.get(35));
            if (tags.containsKey(11))
                rec.put("ClOrdID", tags.get(11));
            if (tags.containsKey(21))
                rec.put("HandlInst", Integer.valueOf(tags.get(21)));
            if (tags.containsKey(55))
                rec.put("Symbol", tags.get(55));
            if (tags.containsKey(54))
                rec.put("Side", Integer.valueOf(tags.get(54)));
            if (tags.containsKey(60))
                rec.put("TransactTime", Long.valueOf(tags.get(60)));
            if (tags.containsKey(38))
                rec.put("OrderQty", Integer.valueOf(tags.get(38)));
            if (tags.containsKey(40))
                rec.put("OrdType", Integer.valueOf(tags.get(40)));
            if (tags.containsKey(10))
                rec.put("CheckSum", tags.get(10));
            if (tags.containsKey(37))
                rec.put("OrderID", tags.get(37));
            if (tags.containsKey(17))
                rec.put("ExecID", tags.get(17));
            if (tags.containsKey(20))
                rec.put("ExecTransType", Integer.valueOf(tags.get(20)));
            if (tags.containsKey(150))
                rec.put("ExecType", Integer.valueOf(tags.get(150)));
            if (tags.containsKey(39))
                rec.put("OrdStatus", Integer.valueOf(tags.get(39)));
            if (tags.containsKey(151))
                rec.put("LeavesQty", Integer.valueOf(tags.get(151)));
            if (tags.containsKey(14))
                rec.put("CumQty", Integer.valueOf(tags.get(14)));
            if (tags.containsKey(6))
                rec.put("AvgPx", Double.valueOf(tags.get(6)));
            rec.put("message", input);
            
            records.add(rec);
        }
        
        return records;
    }

    @Override
    public Object extractGroupByKey(String input) {
        String[] kvps = input.split(String.valueOf((char) 1));
        
        for (String kvp : kvps) {
            String[] components = kvp.split("=");
            if(components[0].equals("11"))
                return components[1];
        }
        
        return null;
    }

    @Override
    public Schema getSchema() {
        if (schema != null) {
            return schema;
        }
        
        schema = SchemaBuilder
                .record("FIX")
                .fields()
                .requiredString("MsgType")
                .optionalString("ClOrdID")
                .optionalInt("HandlInst")
                .optionalString("Symbol")
                .optionalInt("Side")
                .optionalLong("TransactTime")
                .optionalInt("OrderQty")
                .optionalInt("OrdType")
                .optionalString("CheckSum")
                .optionalString("OrderID")
                .optionalString("ExecID")
                .optionalInt("ExecTransType")
                .optionalInt("ExecType")
                .optionalInt("OrdStatus")
                .optionalInt("LeavesQty")
                .optionalInt("CumQty")
                .optionalDouble("AvgPx")
                .requiredString("message")
                .endRecord();
        
        return schema;
    }
    
}