package com.ning.dw.modules.bean;

import java.util.HashMap;
import java.util.Map;

public class ESTable extends Table {
    
    private String indexName;
    
    private String typeName;
    
    private String indexType;
    
    private Map<String,Column> indexColumnMap = new HashMap<String,Column>();
    
    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public Map<String,Column> getIndexColumnMap() {
        return indexColumnMap;
    }

    public void setIndexColumnMap(Map<String,Column> indexColumnMap) {
        this.indexColumnMap = indexColumnMap;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }


}
