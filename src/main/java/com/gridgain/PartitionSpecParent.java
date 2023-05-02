package com.gridgain;

import org.apache.avro.generic.GenericRecord;
import org.apache.ignite.binary.BinaryObjectBuilder;

public abstract class PartitionSpecParent {

    private String partitionSpec1Name_ = null;
    private String partitionSpec1Value_ = null;
    private String partitionSpec2Name_ = null;
    private String partitionSpec2Value_ = null;
    private String partitionSpec3Name_ = null;
    private String partitionSpec3Value_ = null;

    public PartitionSpecParent(String partitionSpec1, String partitionSpec2, String partitionSpec3) {
        if(null != partitionSpec1) {
            String[] elements = partitionSpec1.split("=");
            partitionSpec1Name_ = elements[0];
            partitionSpec1Value_ = elements[1];
        }
        if(null != partitionSpec2) {
            String[] elements = partitionSpec2.split("=");
            partitionSpec2Name_ = elements[0];
            partitionSpec2Value_ = elements[1];
        }
        if(null != partitionSpec3) {
            String[] elements = partitionSpec3.split("=");
            partitionSpec3Name_ = elements[0];
            partitionSpec3Value_ = elements[1];
        }
    }

    protected void maybeAddSpec1(BinaryObjectBuilder builder, GenericRecord record) {
        if(null != partitionSpec1Name_ && null != partitionSpec1Value_) {
            builder.setField(partitionSpec1Name_, partitionSpec1Value_);
        }
    }

    protected void maybeAddSpec2(BinaryObjectBuilder builder, GenericRecord record) {
        if(null != partitionSpec2Name_ && null != partitionSpec2Value_) {
            builder.setField(partitionSpec2Name_, partitionSpec2Value_);
        }
    }
    protected void maybeAddSpec3(BinaryObjectBuilder builder, GenericRecord record) {
        if(null != partitionSpec3Name_ && null != partitionSpec3Value_) {
            builder.setField(partitionSpec3Name_, partitionSpec3Value_);
        }
    }

    protected void maybeAddSpec1(StringBuilder builder) {
        if(null != partitionSpec1Name_) {
            builder.append(",\n  ");
            builder.append(partitionSpec1Name_);
            builder.append(" varchar");
        }
    }

    protected void maybeAddSpec2(StringBuilder builder) {
        if(null != partitionSpec2Name_) {
            builder.append(",\n  ");
            builder.append(partitionSpec2Name_);
            builder.append(" varchar");
        }
    }
    protected void maybeAddSpec3(StringBuilder builder) {
        if(null != partitionSpec3Name_) {
            builder.append(",\n  ");
            builder.append(partitionSpec3Name_);
            builder.append(" varchar");
        }
    }

}
