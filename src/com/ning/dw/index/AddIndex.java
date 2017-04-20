package com.ning.dw.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.ning.dw.es.EsClient;
import com.ning.dw.modules.bean.Column;
import com.ning.dw.modules.bean.Index;
import com.ning.dw.modules.bean.Table;
import com.ning.dw.tools.ConfigReader;
import com.ning.dw.tools.ZookeeperUtils;

public class AddIndex {

    final static public String ES_PREFIX = "elasticsearch_";
    final static public String ES_COLUMN_TYPE = "string";
    private static TransportClient client;
    private HBaseAdmin admin;
    private static final Log LOG = LogFactory.getLog(AddIndex.class);

    private static ZookeeperUtils tableDef = null;

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
                Configuration config = new Configuration();
                config.set("hbase.zookeeper.quorum", ConfigReader.getPropValue("zookeeper.server"));
                admin = new HBaseAdmin(config);
            }
            return admin;
        } catch (IOException ioe) {
            return null;
        }
    }

    // 判断表是否存在
    private boolean isExist(String tableName) {
        HBaseAdmin ha = null;

        ha = getHBaseAdmin();

        try {
            return ha.tableExists(tableName);
        } catch (Exception e) {
            return false;
        }

    }

    public String addGlobalIndex(String tableName, String indexName, String desc) {
        if ("".equals(tableName) || "".equals(indexName) || "".equals(desc)) {
            return "Not allowed null args!!!!";
        }

        String[] indexCols = desc.split("\\|");
        if (indexCols.length < 1) {
            return "Not allowed input with no column!!!";
        }
        try {
            Index index = new Index();
            index.setIndexName(indexName);
            index.setIndexType("global");
            List<Column> indexColumnList = new ArrayList<Column>();

            ZookeeperUtils zkUtils = new ZookeeperUtils();
            Table table = zkUtils.getTableDefWithName(tableName, "hbase");
            Map<String, Column> colMap = table.getColumnMap();
            String[] fnq = null;
            Column col = null;
            Column tableColumn = null;
            for (int i = 0; i < indexCols.length; i++) {
                fnq = indexCols[i].split(":");
                if (fnq != null && fnq.length < 3) {
                    // return ""+fnq.length+";"+fnq[0]+";"+indexCols.length;
                    return "Plz input family and qualify and length!!!";
                }

                for (int j = 0; j < fnq.length; j++) {

                    tableColumn = colMap.get(fnq[1]);
                    col = new Column();
                    col.setFamilyName(fnq[0]);
                    col.setName(fnq[1]);
                    col.setType(ES_COLUMN_TYPE);
                    col.setIndexLength(Integer.parseInt(fnq[2].trim()));
                    col.setHiveName(tableColumn.getHiveName());
                    col.setHiveType(tableColumn.getHiveType());
                }
                indexColumnList.add(col);
            }
            index.setIndexColumnList(indexColumnList);

            HTableDescriptor tableDesc = new HTableDescriptor(indexName);
            tableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes("f")));

            getHBaseAdmin().createTable(tableDesc);

            // LOG.warn("***"+index.getIndexName());
            // LOG.warn("***"+index.getIndexType());
            zkUtils.setGlobalIndexDefine(tableName, index);
        } catch (Exception e) {
            return "Add index fail!!! :" + e.getMessage();
        }
        return "Add index success!!!";
    }

    public String deleteGlobalIndex(String tableName, String indexName) {
        if (isExist(indexName)) {
            try {
                HBaseAdmin admin = getHBaseAdmin();
                admin.disableTable(indexName);
                admin.deleteTable(indexName);
            } catch (IOException e) {
                e.printStackTrace();
                return "Delete index fail!!! :" + e.getMessage();
            }

            try {
                new ZookeeperUtils().deleteGlobalIndexDefine(tableName, indexName);
            } catch (ZooKeeperConnectionException e) {
                return "Delete index fail!!! :" + e.getMessage();
            } catch (KeeperException e) {
                return "Delete index fail!!! :" + e.getMessage();
            } catch (IOException e) {
                return "Delete index fail!!! :" + e.getMessage();
            } catch (InterruptedException e) {
                return "Delete index fail!!! :" + e.getMessage();
            }
        } else {
            return "Index not exists!!!";
        }
        return "Delete index success!!!";
    }

    public String addFulltextIndex(String tableName, String indexName, String desc) {

        if ("".equals(tableName) || "".equals(indexName) || "".equals(desc)) {
            return "Not allowed null args!!!!";
        }

        String[] indexCols = desc.split("\\|");
        if (indexCols.length < 1) {
            return "Not allowed input with no column!!!";
        }

        indexName = ES_PREFIX + indexName;

        Index index = new Index();
        index.setIndexName(indexName);
        index.setIndexType("es");
        List<Column> indexColumnList = new ArrayList<Column>();
        

        XContentBuilder builder;
        try {
            ZookeeperUtils zkUtils = new ZookeeperUtils();
            Table table = zkUtils.getTableDefWithName(tableName, "hbase");
            Map<String, Column> colMap = table.getColumnMap();

            builder = XContentFactory.jsonBuilder().startObject().startObject(indexName)// type名称
                    .startObject("_source").field("enabled", "false").endObject().startObject("properties");

            String[] fnq = null;
            Column col = null;
            Column tableColumn = null;
            for (int i = 0; i < indexCols.length; i++) {
                fnq = indexCols[i].split(":");
                if (fnq != null && fnq.length < 2) {
                    // return ""+fnq.length+";"+fnq[0]+";"+indexCols.length;
                    return "Plz input family and qualify!!!";
                }
                for (int j = 0; j < fnq.length; j++) {
                    builder.startObject(fnq[0] + "_" + fnq[1]).field("type", ES_COLUMN_TYPE).field("store", "false").field("analyzer", "keyword");
                    builder.endObject();
                    tableColumn = colMap.get(fnq[1]);
                    col = new Column();
                    col.setFamilyName(fnq[0]);
                    col.setName(fnq[1]);
                    col.setType(ES_COLUMN_TYPE);
                    col.setHiveName(tableColumn.getHiveName());
                    col.setHiveType(tableColumn.getHiveType());
                }
                indexColumnList.add(col);
            }

            builder.endObject().endObject().endObject();
            index.setIndexColumnList(indexColumnList);

            CreateIndexRequestBuilder cirb = client.admin().indices().prepareCreate(indexName).setSettings(Settings.settingsBuilder()
                    .put("number_of_shards", ConfigReader.getPropValue("shards")).put("number_of_replicas", ConfigReader.getPropValue("replicas")));

            CreateIndexResponse response1 = cirb.execute().actionGet();

            if (!response1.isAcknowledged()) {
                return "Add index fail!!!";
            }

            // 更新mapping
            PutMappingRequest mapping = Requests.putMappingRequest(indexName).type(indexName).source(builder);
            PutMappingResponse response2 = client.admin().indices().putMapping(mapping).actionGet();

            if (!response2.isAcknowledged()) {
                return "Add index fail!!!";
            }

            // LOG.warn("***"+index.getIndexName());
            // LOG.warn("***"+index.getIndexType());
            zkUtils.setESIndexDefine(tableName, index);

        } catch (Exception e) {
            return "Add index fail!!! :" + e.getMessage();
        }
        return "Add index success!!!";
    }

    public String deleteFulltextIndex(String tableName, String indexName)
            throws ZooKeeperConnectionException, KeeperException, IOException, InterruptedException {
        if (client.admin().indices().exists(new IndicesExistsRequest(ES_PREFIX + indexName)).actionGet().isExists()) {
            // 删除ES索引
            DeleteIndexResponse res = client.admin().indices().prepareDelete(ES_PREFIX + indexName).execute().actionGet();
            if (!res.isAcknowledged()) {
                return "Delete index fail!!!";
            }

            new ZookeeperUtils().deleteESIndexDefine(tableName, ES_PREFIX + indexName);

            return "Delete index success!!!";
        } else {
            return "Index dosen't exists!!!";
        }

    }

    public String rebuildGlobalIndex(String tableName, String indexName) throws Exception {
        if ("".equals(tableName) || "".equals(indexName)) {
            return "Not allowed null args!!!!";
        }

        if (isExist(indexName)) {
            try {
                getHBaseAdmin().disableTable(indexName);
                TableName t = TableName.valueOf(indexName);
                getHBaseAdmin().truncateTable(t, false);

                RebuildGlobalIndex rebuildGlobalIndex = new RebuildGlobalIndex();
                rebuildGlobalIndex.getDefIndexColumn(tableName, indexName);
                return rebuildGlobalIndex.Rebuild(tableName, indexName);

            } catch (IOException e) {
                return "Delete index fail!!! :" + e.getMessage();
            }

        } else {
            return "Index dosen't exists!!!";
        }
    }

    public String rebuildFulltextIndex(String tableName, String indexName) throws ClassNotFoundException, IOException, InterruptedException {
        if ("".equals(tableName) || "".equals(indexName)) {
            return "Not allowed null args!!!!";
        }

        if (client.admin().indices().exists(new IndicesExistsRequest(ES_PREFIX + indexName)).actionGet().isExists()) {
            // 删除ES索引
            /*
             * DeleteIndexResponse res =
             * client.admin().indices().prepareDelete(ES_PREFIX +
             * indexName).execute().actionGet(); if (!res.isAcknowledged()) {
             * return "Delete index fail!!!"; }
             */

            RebuildFulltext rebuildFulltext = new RebuildFulltext();
            rebuildFulltext.getDefIndexColumn(tableName, ES_PREFIX + indexName);
            return rebuildFulltext.Rebuild(tableName, ES_PREFIX + indexName);

        } else {
            return "Index dosen't exists!!!";
        }

    }

}
