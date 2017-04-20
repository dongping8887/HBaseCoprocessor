package com.ning.dw.constants;

import com.ning.dw.tools.ConfigReader;

/**
 * 常量接口
 * 
 * @author ningyexin 1
 *
 */
public class Constants
{
    
    public static final String ES_BATCH_SIZE = ConfigReader.getPropValue("es.batch.size");
    
    final static public String ES_PREFIX = "elasticsearch_";
    
}