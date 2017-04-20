package com.ning.dw.modules.bean;

import java.util.ArrayList;
import java.util.List;

public class HbaseTable extends Table {
    
    private List<Index> globalIndexes = new ArrayList<Index>();
    
    private Index esIndex = null;

    private String majorKey;
    
    
    public List<Index> getGlobalIndexes() {
        return globalIndexes;
    }

    public void setGlobalIndexes(List<Index> globalIndexes) {
        this.globalIndexes = globalIndexes;
    }

    public String getMajorKey() {
        return majorKey;
    }

    public void setMajorKey(String majorKey) {
        this.majorKey = majorKey;
    }

    public Index getEsIndex() {
        return esIndex;
    }

    public void setEsIndex(Index esIndex) {
        this.esIndex = esIndex;
    }
}
