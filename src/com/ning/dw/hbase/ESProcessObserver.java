package com.ning.dw.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Map.Entry;

import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.ning.dw.constants.Constants;
import com.ning.dw.es.EsClient;
import com.ning.dw.exception.ESHbaseException;
import com.ning.dw.modules.bean.Column;
import com.ning.dw.modules.bean.HbaseTable;
import com.ning.dw.modules.bean.Index;
import com.ning.dw.modules.bean.Table;
import com.ning.dw.tools.ConfigReader;
import com.ning.dw.tools.ZookeeperUtils;

/**
 * coprocessor新增索引数据操作
 * 
 * @author ningyexin
 *
 */
public class ESProcessObserver extends BaseRegionObserver {

    public static TransportClient client = null;
    private static ZookeeperUtils tableDef = null;
    private static final Log LOG = LogFactory.getLog(ESProcessObserver.class);
    private static Configuration hbaseConfig = HBaseConfiguration.create();
    private static Connection hbaseConnection;
    private static final String bufferSize = ConfigReader.getPropValue("hbase.buffer.size");
    private static String ESSwitch = ConfigReader.getPropValue("es_switch");

    static {
        if ("true".equals(ESSwitch)) {
            try {
                client = EsClient.getInstance();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            hbaseConnection = ConnectionFactory.createConnection(hbaseConfig);
        } catch (IOException e) {

        }
        
        tableDef = new ZookeeperUtils();
        
        

    }

//    @Override
//    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
//        int dataSize = miniBatchOp.size();
//        for (int i = 0; i < array.length; i++) {
//            
//        }
//    }
    
    /**
     * BaseRegionObserver上的注释 
     * This will be called after applying a batch of
     * Mutations on a region. The Mutations are added to memstore and WAL.
     * 
     * Overrides: postBatchMutate(...) in BaseRegionObserver Parameters: ctx the
     * environment provided by the region server miniBatchOp batch of Mutations
     * applied to region.
     * 
     * @Parameters:
     * @param ctx
     *            环境上下文
     * @param miniBatchOp
     *            region里操作的一批数据
     */
    @Override
    public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> ctx, MiniBatchOperationInProgress<Mutation> miniBatchOp) {

        try {
            // 获取表名
            Region userRegion = ctx.getEnvironment().getRegion();
            HTableDescriptor userTableDesc = userRegion.getTableDesc();
            String tableName = userTableDesc.getNameAsString();
            // LOG.warn("**************************in!!!******************************");
            int dataSize = miniBatchOp.size();
            try {
                HbaseTable table = tableDef.getHbaseTableDefWithName(tableName);
                if(table== null){
                    return;
                }
                List<Index> idxList = table.getGlobalIndexes();
                // LOG.warn("**************************idxList:"+idxList.size()+"******************************");
                if (idxList.size() > 0) {
                    for (int k = 0; k < idxList.size(); k++) {
                        Index index = idxList.get(k);
                        List<Column> colList = index.getIndexColumnList();
                        globalIndexer(miniBatchOp, dataSize, index, colList);
                    }
                }
                Index esIndex = table.getEsIndex();

                if (esIndex != null && esIndex.getIndexName() != null) {
                    String indexName = esIndex.getIndexName();
                    esIndexer(miniBatchOp, tableName, dataSize, indexName, esIndex.getIndexColumnList());
                }

            } catch (Exception e) {
                LOG.warn("**************************" + e.getMessage() + "******************************");
                LOG.warn("**************************" + e.getCause() + "******************************");
            }
        } catch (Exception e) {
            LOG.warn("**************************" + e.getMessage() + "******************************");
            LOG.warn("**************************" + e.getCause() + "******************************");
        }
    }

    private void globalIndexer(MiniBatchOperationInProgress<Mutation> miniBatchOp, int dataSize, Index index, List<Column> colList)
            throws IOException, ScriptException {
        HTable indexTable = (HTable) hbaseConnection.getTable(TableName.valueOf(index.getIndexName()));
        indexTable.setAutoFlush(false, true);
        double tmpDubl = (Double) new ScriptEngineManager().getEngineByName("JavaScript").eval(bufferSize.trim());
        indexTable.setWriteBufferSize((int) tmpDubl);
        // LOG.warn("**************************index:"+index.getIndexName()+"******************************");
        List<Put> puts = new ArrayList<Put>();
        Put put = null;
        String indexRow = null;
        Column col = null;
        for (int i = 0; i < dataSize; i++) {

            Mutation mutation = miniBatchOp.getOperation(i);
            NavigableMap<byte[], List<Cell>> values = mutation.getFamilyCellMap();
            // LOG.warn("**************************values:"+values+"******************************");
            indexRow = "";
            String rowKey = "";
            String[] cacheRow = new String[colList.size()];
            String midCache = null;
            for (Map.Entry<byte[], List<Cell>> value : values.entrySet()) {
                // byte[] rowkey = value.getKey();
                List<Cell> valuess = value.getValue();

                for (Cell kv : valuess) {
                    byte bb = kv.getTypeByte();

                    /**
                     * Minimum((byte)0), Put((byte)4), Delete((byte)8),
                     * DeleteFamilyVersion((byte)10), DeleteColumn((byte)12),
                     * DeleteFamily((byte)14),
                     * 
                     * Maximum is used when searching; you look from maximum on
                     * down. Maximum((byte)255);
                     */

                    if (bb == 4) {
                        String valueBs = Bytes.toString(kv.getValue());
                        rowKey = Bytes.toString(kv.getRow());
                        String family = Bytes.toString(kv.getFamily());
                        String qualifier = Bytes.toString(kv.getQualifier());
                        // LOG.warn("**************************valueBs:"+valueBs+"******************************");
                        for (int j = 0; j < colList.size(); j++) {
                            col = colList.get(j);
                            if (col.getFamilyName().equals(family) && col.getName().equals(qualifier)) {
                                // LOG.warn("**************************cacheRow:"+cacheRow[j]+"******************************");
                                cacheRow[j] = valueBs;
                            }
                        }

                    } else if (bb == 8) {

                    }
                }
            }
            for (int j = 0; j < colList.size(); j++) {
                col = colList.get(j);
                midCache = "";
                if (col.getIndexLength() > cacheRow[j].length()) {
                    for (int j2 = 0; j2 < col.getIndexLength() - cacheRow[j].length(); j2++) {
                        midCache += new Character((char) 0);
                    }
                    indexRow += cacheRow[j] + midCache;

                } else if (col.getIndexLength() < cacheRow[j].length()) {
                    indexRow += cacheRow[j].substring(0, col.getIndexLength());
                } else {
                    indexRow += cacheRow[j];
                }
                indexRow += new Character((char) 127);
            }
            indexRow += rowKey;
            // LOG.warn("**************************indexRow:"+indexRow+"******************************");
            put = new Put(indexRow.getBytes());
            put.addColumn("f".getBytes(), "q".getBytes(), "n".getBytes());
            puts.add(put);

        }
        indexTable.put(puts);
        indexTable.flushCommits();
        indexTable.close();
    }

    private void esIndexer(MiniBatchOperationInProgress<Mutation> miniBatchOp, String tableName, int dataSize, String indexName, List<Column> colList)
            throws IOException, ESHbaseException {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        // 批量入es个数
        int batch = Integer.parseInt(Constants.ES_BATCH_SIZE);
        XContentBuilder builder = null;
        // IndexResponse response = null;
        // 默认 columns[0] �?Object[0] �?rowkey字段和�?

        Column col = null;
        for (int i = 0; i < dataSize; i++) {
            builder = XContentFactory.jsonBuilder().startObject();

            Mutation mutation = miniBatchOp.getOperation(i);

            NavigableMap<byte[], List<Cell>> values = mutation.getFamilyCellMap();

            String rowKey = "";
            for (Map.Entry<byte[], List<Cell>> value : values.entrySet()) {
                // byte[] rowkey = value.getKey();
                List<Cell> valuess = value.getValue();
                for (Cell kv : valuess) {
                    byte bb = kv.getTypeByte();
                    /**
                     * Minimum((byte)0), Put((byte)4), Delete((byte)8),
                     * DeleteFamilyVersion((byte)10), DeleteColumn((byte)12),
                     * DeleteFamily((byte)14),
                     * 
                     * Maximum is used when searching; you look from maximum on
                     * down. Maximum((byte)255);
                     */
                    if (bb == 4) {

                        String valueBs = Bytes.toString(kv.getValue());
                        rowKey = Bytes.toString(kv.getRow());
                        String family = Bytes.toString(kv.getFamily());
                        String qualifier = Bytes.toString(kv.getQualifier());
                        // LOG.warn("**************************columnSize:"+columnSize+";splitVals:"+splitVals.length+"******************************");

                        for (int j = 0; j < colList.size(); j++) {
                            col = colList.get(j);
                            if (col.getFamilyName().equals(family) && col.getName().equals(qualifier)) {
                                // LOG.warn(i+"*************************Im
                                // in"+family + "_" + qualifier+" : "
                                // +valueBs+"********************");
                                builder.field(family + "_" + qualifier, valueBs);
                            }
                        }

                    } else if (bb == 8) {
                        try {
                            Delete delete = (Delete) mutation;
                            client.delete(new DeleteRequest(indexName, indexName, Bytes.toString(delete.getRow())));
                            client.delete(new DeleteRequest(indexName, indexName, ""));
                        } catch (Exception e) {

                        }
                    }
                }

            }
            builder.endObject();
            bulkRequest.add(client.prepareIndex(indexName, indexName, rowKey).setSource(builder));
            
            if (i % batch == 0) {
                BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                if (bulkResponse.hasFailures()) {
                    // System.err.println("批量添加出错");
                    errorMsg(ESHbaseException.INDEX_INSERT_FAIL, tableName);
                }
                bulkRequest.request().requests().clear();

            }
        }

        try {
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            if (bulkResponse.hasFailures()) {
                // System.err.println("批量添加出错");
                errorMsg(ESHbaseException.INDEX_INSERT_FAIL, tableName);
            }
        } catch (Exception e) {

        }

        bulkRequest.request().requests().clear();

    }

    /**
     * 删除数据的协处理
     * 
     */
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        try {
            // 获取表名
            Region userRegion = e.getEnvironment().getRegion();
            HTableDescriptor userTableDesc = userRegion.getTableDesc();
            String tableName = userTableDesc.getNameAsString();

            HbaseTable table = tableDef.getHbaseTableDefWithName(tableName);
            if ("true".equals(ESSwitch)) {
                Index esIndex = table.getEsIndex();
                String indexName = esIndex.getIndexName();
                client.delete(new DeleteRequest(indexName, indexName, Bytes.toString(delete.getRow())));
                client.delete(new DeleteRequest(indexName, indexName, ""));
            }
        } catch (Exception e1) {
            LOG.warn("**************************" + e1.getMessage() + "******************************");
            LOG.warn("**************************" + e1.getCause() + "******************************");
        }
    }

    private static void errorMsg(String msg, String... params) throws ESHbaseException {
        LOG.error(ESHbaseException.buildMsg(msg, params));
        throw ESHbaseException.throwException(msg, params);
    }

}
