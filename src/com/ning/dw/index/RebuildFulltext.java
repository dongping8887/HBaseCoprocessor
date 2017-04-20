package com.ning.dw.index;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.ning.dw.constants.Constants;
import com.ning.dw.es.EsClient;
import com.ning.dw.modules.bean.Column;
import com.ning.dw.modules.bean.HbaseTable;
import com.ning.dw.modules.bean.Index;
import com.ning.dw.modules.bean.Table;
import com.ning.dw.tools.ZookeeperUtils;

public class RebuildFulltext {

    private HBaseAdmin admin;
    private static final Log LOG = LogFactory.getLog(AddIndex.class);
    
    private static String rebuildTableName;
    private static String rebuildIndexTableName;
    
    private static List<Column> listDefIndexColumn = null;
    
    private static TransportClient client;
    
    private static BulkRequestBuilder bulkRequest;
    
    private static int i =0;
    
    static {
        try {
            client = EsClient.getInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    private HBaseAdmin getHBaseAdmin() {
        try {
            if (admin == null) {
                admin = new HBaseAdmin(new Configuration());
            }
            return admin;
        } catch (IOException ioe) {
            return null;
        }
    }

    public void getDefIndexColumn(String tableName, String indexName){
        rebuildTableName = tableName;
        rebuildIndexTableName = indexName;
        try {
            ZookeeperUtils tableDef = new ZookeeperUtils();
            HbaseTable table = tableDef.getHbaseTableDefWithName(rebuildTableName);
            
            Index index = table.getEsIndex();
            if(index.getIndexName().equals(rebuildIndexTableName)){
                listDefIndexColumn = index.getIndexColumnList();
                LOG.warn("***get global index definition information sucessfully!!!"+listDefIndexColumn.size());
            }

        } catch (Exception e) {
            e.printStackTrace();
        } 
    }  
    
    /*public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        // TODO Auto-generated method stub
        
        RebuildFulltext rebuildFulltext = new RebuildFulltext();
        
        rebuildFulltext.getDefIndexColumn("test11","elasticsearch_test11");
        String msg = "";
        msg = rebuildFulltext.Rebuild("test11", "elasticsearch_test11");
    }*/

 
    public String Rebuild(String tableName, String indexName) throws IOException, ClassNotFoundException, InterruptedException {
        rebuildTableName = tableName;
        rebuildIndexTableName = indexName;
        
        @SuppressWarnings("deprecation")
        Job job = new Job(getHBaseAdmin().getConfiguration(),"rebuild_global_index " + rebuildTableName + " " + rebuildIndexTableName);
//        Job job = new Job( Table1Define.hConfig,"rebuild_fulltext_index " + rebuildTableName + " " + rebuildIndexTableName);
 
        job.setJarByClass(RebuildFulltext.class);
 
        job.setMapperClass(MyMapper.class);
        job.setNumReduceTasks(0);//由于不需要执行reduce阶段
 
        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(MultiTableOutputFormat.class);
 
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob(rebuildTableName,scan, 
                MyMapper.class, ImmutableBytesWritable.class, Put.class, job);
 
        job.waitForCompletion(true);
        
        return "Rebuild fulltext index successfully!!!";
         
    }
    
    static class MyMapper extends TableMapper<ImmutableBytesWritable, Put>{
        
        @Override
        protected void map(ImmutableBytesWritable key, Result value,
                Context context) throws IOException, InterruptedException {
//            BulkRequestBuilder bulkRequest = client.prepareBulk();
            
            XContentBuilder builder = null;
            builder = XContentFactory.jsonBuilder().startObject();
                
            // 批量入es个数
            int batch = Integer.parseInt(Constants.ES_BATCH_SIZE);
//            int batch = 999999;
            
            Column col = null;
            String orignalRowKey = new String(key.get(),"UTF-8");
            Column colDefIndex = null;
            String lsIndexName;
            String lsIndexValue;
            
            for (int i = 0; i < listDefIndexColumn.size(); i++) {
                colDefIndex = listDefIndexColumn.get(i);
                lsIndexName = colDefIndex.getName();
                //取的原表value
                byte[] val = value.getValue(Bytes.toBytes(colDefIndex.getFamilyName()), lsIndexName.getBytes());
                if(val == null){
                    return;
                }
                lsIndexValue = new String(val,"UTF-8");
                if(null != lsIndexValue){
                    builder.field(colDefIndex.getFamilyName() + "_" + lsIndexName, lsIndexValue);
                }
            }  
            
            builder.endObject();

            bulkRequest.add(client.prepareIndex(rebuildIndexTableName,rebuildIndexTableName, orignalRowKey)
                    .setSource(builder));
            i += 1;
            if (i % batch == 0) {
                BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                if (bulkResponse.hasFailures()) {
                    LOG.error("批量添加出错");
                }
                bulkRequest.request().requests().clear();
                i = 0;
            }
        }
        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            bulkRequest = client.prepareBulk();
        }
        
        @Override 
        protected void cleanup(Context cxt) throws IOException,
        InterruptedException{
          BulkResponse bulkResponse = bulkRequest.execute().actionGet();
          if (bulkResponse.hasFailures()) {
              LOG.error("最后一批量添加出错");
          }
          bulkRequest.request().requests().clear();
          LOG.info("ParseSinglePutDriver cleanup ,current time :"+new Date());
        }
        
    }
    

    
}