package com.cloudera.fce.nrtkudu.fix;

import java.util.List;

import org.apache.avro.generic.GenericRecord;

import com.cloudera.fce.nrtkudu.KuduEncoder;
import com.google.common.collect.Lists;

public class ExecRptKuduEncoder extends KuduEncoder {
    
    @Override
    protected String getTableName() {
        return "execrpt";
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
        // Value fields are fields that are used in change data capture
        
        List<String> valueFields = Lists.newArrayList();
        
        valueFields.add("orderid");
        valueFields.add("execid");
        valueFields.add("exectranstype");
        valueFields.add("exectype");
        valueFields.add("ordstatus");
        valueFields.add("symbol");
        valueFields.add("side");
        valueFields.add("leavesqty");
        valueFields.add("cumqty");
        valueFields.add("avgpx");
        valueFields.add("transacttime");
        
        return valueFields;
    }

    @Override
    protected Object deriveOutputField(GenericRecord input, String outputFieldName) {
        Object value = null;
        
        switch (outputFieldName) {
            case "clordid":
                value = input.get("ClOrdID");
                break;
            case "orderid":
                value = input.get("OrderID");
                break;
            case "execid":
                value = input.get("ExecID");
                break;
            case "exectranstype":
                value = input.get("ExecTransType");
                break;
            case "exectype":
                value = input.get("ExecType");
                break;
            case "ordstatus":
                value = input.get("OrdStatus");
                break;
            case "symbol":
                value = input.get("Symbol");
                break;
            case "side":
                value = input.get("Side");
                break;
            case "leavesqty":
                value = input.get("LeavesQty");
                break;
            case "cumqty":
                value = input.get("CumQty");
                break;
            case "avgpx":
                value = input.get("AvgPx");
                break;
            case "transacttime":
                value = input.get("TransactTime");
                break;
            default:
                break;
        }
        
        return value;
    }
    
    @Override
    protected boolean isApplicableForOutput(GenericRecord input) {
        return input.get("MsgType").equals("8");
    }
    
    @Override
    protected boolean doesContainUpdates() {
        return true;
    }

    @Override
    protected boolean doesTrackHistory() {
        return true;
    }

//    @Override
//    protected String getValueHashFieldName() {
//        return null;
//    }

}
