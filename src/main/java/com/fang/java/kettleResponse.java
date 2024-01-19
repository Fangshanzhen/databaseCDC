package com.fang.java;

import lombok.Data;
import org.pentaho.di.core.row.RowMetaInterface;

@Data
public class kettleResponse {
    Object[] outputRowData;
    RowMetaInterface outputRowMeta;

    public kettleResponse(){}
}
