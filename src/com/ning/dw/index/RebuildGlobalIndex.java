package com.ning.dw.index;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ning.dw.modules.bean.Column;
import com.ning.dw.modules.bean.HbaseTable;
import com.ning.dw.modules.bean.Index;
import com.ning.dw.modules.bean.Table;
import com.ning.dw.tools.ZookeeperUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class RebuildGlobalIndex {

    private static String outPathStr = "/hbaseIndexRebuildTmp";
    private HBaseAdmin admin;
    private static final Log LOG = LogFactory.getLog(AddIndex.class);

    private static String rebuildTableName;
    private static String rebuildIndexTableName;

    private static List<Column> listDefIndexColumn = null;

    private static Connection conn = null;

    private static Configuration conf = new Configuration();

    static {
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private HBaseAdmin getHBaseAdmin() {
        try {
            if (admin == null) {
                admin = (HBaseAdmin) conn.getAdmin();
            }
            return admin;
        } catch (IOException ioe) {
            return null;
        }
    }

    // public static void main(String[] args) throws ClassNotFoundException,
    // IOException, InterruptedException {
    // // TODO Auto-generated method stub
    //
    // RebuildGlobalIndex rebuildGlobalIndex = new RebuildGlobalIndex();
    //
    // rebuildGlobalIndex.getDefIndexColumn("test_people","test_people_name");
    // String msg = "";
    // msg = rebuildGlobalIndex.Rebuild("test_people", "test_people_name");
    //
    // }

    public String Rebuild(String tableName, String indexName) {

        rebuildTableName = tableName;
        rebuildIndexTableName = indexName;

        HTable hDataTable;
        HTable hIndexTable;
        try {
            hDataTable = (HTable) conn.getTable(TableName.valueOf(tableName));
            hIndexTable = (HTable) conn.getTable(TableName.valueOf(indexName));

            
            Configuration config = new Configuration();  
//            Configuration config = getHBaseAdmin().getConfiguration();
            String rootPath=System.getenv("HADOOP_HOME")+"/";  
            config.addResource(new Path(rootPath+"yarn-site.xml"));  
            config.addResource(new Path(rootPath+"core-site.xml"));  
            config.addResource(new Path(rootPath+"hdfs-site.xml"));  
            config.addResource(new Path(rootPath+"mapred-site.xml"));  
            
            @SuppressWarnings("deprecation")
            Job job = new Job(config, "rebuild_global_index " + rebuildTableName + " " + rebuildIndexTableName);

            job.setJarByClass(RebuildGlobalIndex.class);

            job.setNumReduceTasks(0);// 由于不需要执行reduce阶段

            job.setInputFormatClass(TableInputFormat.class);
            job.setOutputFormatClass(MultiTableOutputFormat.class);

//            Path outPath = new Path(new Path(outPathStr), Bytes.toString(TableName.valueOf(indexName).getQualifier()) + System.currentTimeMillis());
//            FileSystem fs = outPath.getFileSystem(conf);
            // fs.delete(outPath);
//            fs.delete(outPath, true);
//            FileOutputFormat.setOutputPath(job, outPath);

            job.setMapperClass(MyMapper.class);
//            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
//            job.setMapOutputValueClass(Put.class);

//            HFileOutputFormat2.configureIncrementalLoad(job, hDataTable);
            Scan scan = new Scan();
            TableMapReduceUtil.initTableMapperJob(rebuildTableName, scan, MyMapper.class, ImmutableBytesWritable.class, Put.class, job);
            
            job.waitForCompletion(true);
//            if (job.waitForCompletion(true)) {
////                LOG.warn("****"+outPath.toString());
//                
//                LoadIncrementalHFiles loader = new LoadIncrementalHFiles(HBaseConfiguration.create());
//                loader.doBulkLoad(outPath, hIndexTable);
//            }
            
            return "Rebuild index successfully!!!";
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return e.getMessage();
        }
    }

    static class MyMapper extends TableMapper<ImmutableBytesWritable, Put> {

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

            String orignalRowKey = new String(key.get(), "UTF-8");

            // 索引表的rowkey是原始表的列，索引表的列是原始表的rowkey
            String midCache = ""; // 拼接字符串
            String indexRowKey = "";
            Column colDefIndex = null;
            String lsIndexName;
            String lsIndexValue;
            int lnIndexLength;
            int lnValueLength;

            for (int j = 0; j < listDefIndexColumn.size(); j++) {
                colDefIndex = listDefIndexColumn.get(j);
                midCache = "";
                lsIndexName = colDefIndex.getName();
                lnIndexLength = colDefIndex.getIndexLength();
                // 取的原表value
                byte[] val = value.getValue(Bytes.toBytes(colDefIndex.getFamilyName()), lsIndexName.getBytes());
                
                if(val == null){
                    return;
                }
                lsIndexValue = new String(val, "UTF-8");
                lnValueLength = lsIndexValue.length();
                if (lnIndexLength > lnValueLength) {
                    for (int i = 0; i < lnIndexLength - lnValueLength; i++) {
                        midCache += new Character((char) 0);
                    }
                    indexRowKey += lsIndexValue + midCache;
                } else if (lnIndexLength < lnValueLength) {
                    indexRowKey += lsIndexValue.substring(0, lnIndexLength);
                } else {
                    indexRowKey += lsIndexValue;
                }
                indexRowKey += new Character((char) 127);
            }
            indexRowKey += orignalRowKey;
            Put put = new Put(indexRowKey.getBytes());
            // 列族 列 原始表的行键
            // 添加空的key和value
            put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("q"), Bytes.toBytes(""));

//            if (put != null) {
//                ImmutableBytesWritable outputKey = new ImmutableBytesWritable(put.getRow());
//                try {
//                    context.write(outputKey, put);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
            context.write(new ImmutableBytesWritable(Bytes.toBytes(rebuildIndexTableName)), put);
            // context.write(outputKey, indexPut);
            // context.write(new
            // ImmutableBytesWritable(Bytes.toBytes(rebuildIndexTableName)),
            // put);
        }
    }

    public void getDefIndexColumn(String tableName, String indexName) {
        rebuildTableName = tableName;
        rebuildIndexTableName = indexName;
        try {
            ZookeeperUtils tableDef = new ZookeeperUtils();
            HbaseTable table = tableDef.getHbaseTableDefWithName(rebuildTableName);

            List<Index> idxList = table.getGlobalIndexes();
            Index index;
            if (idxList.size() > 0) {
                for (int k = 0; k < idxList.size(); k++) {
                    index = idxList.get(k);
                    
                    if (index.getIndexName().equals(rebuildIndexTableName)) {
                        listDefIndexColumn = index.getIndexColumnList();
                        LOG.warn("***get global index definition information sucessfully!!!" + listDefIndexColumn.size());
                    }
                    
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}