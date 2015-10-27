package com.cloudera.fce.nrtkudu.fix;

import java.util.List;

import org.apache.avro.generic.GenericRecord;

import com.cloudera.fce.nrtkudu.KuduEncoder;
import com.google.common.collect.Lists;

public class OrdersKuduEncoder extends KuduEncoder {
    
    @Override
    protected String getTableName() {
        return "fix_orders";
    }

    @Override
    protected List<String> getKeyFieldNames() {
        return Lists.newArrayList("clordid");
    }

    @Override
    protected String getEffectiveFromFieldName() {
        return "start_ts";
    }

    @Override
    protected String getEffectiveToFieldName() {
        return "end_ts";
    }

    @Override
    protected String getCurrentFlagFieldName() {
        return "current_flag";
    }

    @Override
    protected String getLastUpdatedFieldName() {
        return "last_updated";
    }

    @Override
    protected List<String> getValueFieldNames() {        
        List<String> valueFields = Lists.newArrayList();
        
        valueFields.add("handlinst");
        valueFields.add("symbol");
        valueFields.add("side");
        valueFields.add("transacttime");
        valueFields.add("orderqty");
        valueFields.add("ordtype");

        return valueFields;
    }

    @Override
    protected Object deriveOutputField(GenericRecord input, String outputFieldName) {
        Object value = null;
        
        switch (outputFieldName) {
            case "clordid":
                value = input.get("ClOrdID");
                break;
            case "handlinst":
                value = input.get("HandlInst");
                break;
            case "symbol":
                value = input.get("Symbol");
                break;
            case "side":
                value = input.get("Side");
                break;
            case "transacttime":
                value = input.get("TransactTime");
                break;
            case "orderqty":
                value = input.get("OrderQty");
                break; 
            case "ordtype":
                value = input.get("OrdType");
                break;
            case "start_ts":
                value = input.get("TransactTime");
                break;
            default:
                break;
        }
        
        return value;
    }
    
    @Override
    protected boolean isApplicableForOutput(GenericRecord input) {
        return input.get("MsgType").equals("D");
    }

    @Override
    protected boolean doesTrackHistory() {
        return true;
    }

}
