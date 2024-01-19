package com.fang.java;


import org.pentaho.di.core.row.RowMetaInterface;


public class kettleResponse {
    public Object[] getOutputRowData() {
        return outputRowData;
    }

    public void setOutputRowData(Object[] outputRowData) {
        this.outputRowData = outputRowData;
    }

    Object[] outputRowData;

    public RowMetaInterface getOutputRowMeta() {
        return outputRowMeta;
    }

    public void setOutputRowMeta(RowMetaInterface outputRowMeta) {
        this.outputRowMeta = outputRowMeta;
    }

    RowMetaInterface outputRowMeta;

    public kettleResponse(){}
}
