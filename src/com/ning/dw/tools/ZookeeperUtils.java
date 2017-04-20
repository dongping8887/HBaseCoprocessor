package com.ning.dw.tools;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NoNodeException;
import com.google.gson.Gson;
import com.ning.dw.exception.ESHbaseException;
import com.ning.dw.modules.bean.Column;
import com.ning.dw.modules.bean.ESTable;
import com.ning.dw.modules.bean.HbaseTable;
import com.ning.dw.modules.bean.Index;
import com.ning.dw.modules.bean.Table;

public class ZookeeperUtils {


    /**
     * @Fields nameSpace : 使用es
     **/
    private final String esNameSpace = "es";

    /**
     * @Fields nameSpace : 使用hbase
     **/
    private final String hbaseNameSpace = "hbase";

    /**
     * @Fields zookeeper :
     **/
    private ZooKeeper zookeeper;
    
    /**
     * @Fields metadataZNode : 元数据信息
     **/
    private String metadataZNode = "/hbase/metadata";

    /**
     * @Fields hbaseConfig : hbaseConfig
     **/
    private static Configuration hConfig = null;
    
    /**
     * LOG
     */
    private static final Log LOG = LogFactory.getLog(ZookeeperUtils.class);
    
    private Watcher watcher =  new Watcher() {  
        
        public void process(WatchedEvent event) {  
            LOG.debug("process : " + event.getType());  
        }  
    };
    
    /**
     * 
      * 创建一个新的实例 ZookeeperUtils. 
      * <p>Title: </p>
      * <p>Description: </p>
      * @throws KeeperException
      * @throws ZooKeeperConnectionException
      * @throws IOException
     * @throws InterruptedException 
     */
    public ZookeeperUtils() {
        hConfig = new Configuration();
        
        try {
            this.zookeeper = new ZooKeeper(ConfigReader.getPropValue("zookeeper.server"), 600000, watcher);
        } catch (IOException e) {
            this.zookeeper = null;
        }
        try {
            this.zookeeper.create(this.metadataZNode, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
        }
        try {
            this.zookeeper.create(this.metadataZNode+"/"+esNameSpace, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
        }
        try {
            this.zookeeper.create(this.metadataZNode+"/"+hbaseNameSpace, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
        }
//        this.zookeeper = new ZooKeeperWatcher(hConfig, "admin", null);
//        metadataZNode = ZKUtil.joinZNode(this.zookeeper.baseZNode, "metadata");
        
//        ZKUtil.createAndFailSilent(this.zookeeper, this.metadataZNode);
    }

    /**
     * 
      * @Title: setESTableDefine
      * @Description: TODO
      * @param table
     * @throws InterruptedException 
      * @throws KeeperException
     */
    public void setESTableDefine(ESTable table) throws KeeperException, InterruptedException {
        String tableName = table.getName();
        String namespacePath = ZKUtil.joinZNode(this.metadataZNode, esNameSpace);
        String tablePath = ZKUtil.joinZNode(namespacePath, tableName);
        Gson gson = new Gson();
        String data = gson.toJson(table);

        this.zookeeper.create(tablePath, Bytes.toBytes(data), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        ZKUtil.createWithParents(this.zookeeper, tablePath, Bytes.toBytes(data));
    }

    /**
     * 
      * @Title: setHbaseTableDefine
      * @Description: TODO
      * @param table
      * @param columns
      * @throws KeeperException
      * @throws IOException
     * @throws InterruptedException 
     */
    public void setHbaseTableDefine(HTable table, List<Column> columns) throws KeeperException, InterruptedException, IOException {
        if (table == null) {
            throw new IllegalArgumentException("null pointer");
        }
        TableName tableName = table.getTableDescriptor().getTableName();
        String namespacePath = ZKUtil.joinZNode(this.metadataZNode, hbaseNameSpace);
        String tablePath = ZKUtil.joinZNode(namespacePath, tableName.getNameAsString());
        HbaseTable tableDef = new HbaseTable();
        tableDef.setColumnList(columns);
        tableDef.setName(tableName.getNameAsString());
        Gson gson = new Gson();
        String data = gson.toJson(tableDef);

        this.zookeeper.create(tablePath, Bytes.toBytes(data), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        ZKUtil.createWithParents(this.zookeeper, tablePath, Bytes.toBytes(data));
    }
    
    /**
     * 
      * @Title: setHbaseTableDefine
      * @Description: TODO
      * @param table
      * @param columns
      * @throws KeeperException
      * @throws IOException
     * @throws InterruptedException 
     */
    public void setHbaseTableDefine(HbaseTable table) throws KeeperException, IOException, InterruptedException {
        if (table == null) {
            throw new IllegalArgumentException("null pointer");
        }
        String tableName = table.getName();
        String namespacePath = ZKUtil.joinZNode(this.metadataZNode, hbaseNameSpace);
        String tablePath = ZKUtil.joinZNode(namespacePath,tableName);
        Gson gson = new Gson();
        String data = gson.toJson(table);

        this.zookeeper.create(tablePath, Bytes.toBytes(data), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        ZKUtil.createWithParents(this.zookeeper, tablePath, Bytes.toBytes(data));
    }

    /**
     * 
      * @Title: setESIndexDefine
      * @Description: TODO
      * @param tableName
      * @param index
      * @throws KeeperException
      * @throws IOException
      * @throws InterruptedException
     */
    public void setESIndexDefine(String tableName, Index index) throws KeeperException, IOException, InterruptedException {
        if (tableName == null) {
            throw new IllegalArgumentException("null tableName");
        }
        HbaseTable table = getHbaseTableDefWithName(tableName);

        table.setEsIndex(index);
        deleteAndSaveTable(tableName, table);
    }

    /**
     * 
      * @Title: setGlobalIndexDefine
      * @Description: TODO
      * @param tableName
      * @param index
      * @throws KeeperException
      * @throws IOException
      * @throws InterruptedException
     */
    public void setGlobalIndexDefine(String tableName, Index index) throws KeeperException, IOException, InterruptedException {
        if (tableName == null) {
            throw new IllegalArgumentException("null tableName");
        }
        HbaseTable table = getHbaseTableDefWithName(tableName);

        table.getGlobalIndexes().add(index);
        deleteAndSaveTable(tableName, table);
    }

    /**
     * 
      * @Title: deleteAndSaveTable
      * @Description: TODO
      * @param tableName
      * @param table
      * @throws IOException
      * @throws KeeperException
     * @throws InterruptedException 
     */
    private void deleteAndSaveTable(String tableName, HbaseTable table) throws IOException, KeeperException, InterruptedException {
        deleteTableDefine(tableName, hbaseNameSpace);

        String namespacePath = ZKUtil.joinZNode(this.metadataZNode, hbaseNameSpace);
        String tablePath = ZKUtil.joinZNode(namespacePath, tableName);
        Gson gson = new Gson();
        String data = gson.toJson(table);
        
        this.zookeeper.create(tablePath, Bytes.toBytes(data), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        ZKUtil.createWithParents(this.zookeeper, tablePath, Bytes.toBytes(data));
    }

    /**
     * 
      * @Title: deleteESIndexDefine
      * @Description: TODO
      * @param tableName
      * @param indexName
      * @throws KeeperException
      * @throws IOException
      * @throws InterruptedException
     */
    public void deleteESIndexDefine(String tableName, String indexName) throws KeeperException, IOException, InterruptedException {
        if (tableName == null) {
            throw new IllegalArgumentException("null tableName");
        }
        HbaseTable table = getHbaseTableDefWithName(tableName);
        table.setEsIndex(null);
        
        deleteAndSaveTable(tableName, table);
    }
    
    /**
     * 
      * @Title: deleteGlobalIndexDefine
      * @Description: TODO
      * @param tableName
      * @param indexName
      * @throws KeeperException
      * @throws IOException
      * @throws InterruptedException
     */
    public void deleteGlobalIndexDefine(String tableName, String indexName) throws KeeperException, IOException, InterruptedException {
        if (tableName == null) {
            throw new IllegalArgumentException("null tableName");
        }
        HbaseTable table = getHbaseTableDefWithName(tableName);
        List<Index> indexList = table.getGlobalIndexes();
        Index oldIndex = null;
        for (int i = 0; i < indexList.size(); i++) {
            oldIndex = indexList.get(i);
            if (oldIndex.getIndexName().equals(indexName)) {
                indexList.remove(i);
            }
        }
        deleteAndSaveTable(tableName, table);
    }

    /**
     * 
      * @Title: getFullTableDefine
      * @Description: TODO
      * @param table
      * @return
      * @throws IOException
      * @throws KeeperException
      * @throws InterruptedException
     */
    public HbaseTable getFullTableDefine(HTable table) throws IOException, KeeperException, InterruptedException {
        if (table == null) {
            throw new IllegalArgumentException("null pointer");
        }
        TableName tableName = table.getName();
        HbaseTable tableDef = (HbaseTable) getTableDefWithName(tableName.getNameAsString(),hbaseNameSpace);

        return tableDef;
    }

    /**
     * 
      * @Title: getHbaseTableDefWithName
      * @Description: TODO
      * @param tableName
      * @return
      * @throws KeeperException
      * @throws InterruptedException
     */
    public HbaseTable getHbaseTableDefWithName(String tableName) throws KeeperException, InterruptedException {
        String tablePath = getMetaPathInZK(tableName, hbaseNameSpace);
        String tableString = Bytes.toString(this.zookeeper.getData(tablePath, true, null));
        Gson gson = new Gson();
        HbaseTable tableDef = gson.fromJson(tableString, HbaseTable.class);
        return tableDef;
    }

    /**
     * 
      * @Title: getESTableDefWithName
      * @Description: TODO
      * @param tableName
      * @return
      * @throws KeeperException
      * @throws InterruptedException
     */
    public ESTable getESTableDefWithName(String tableName) throws KeeperException, InterruptedException {
        String tablePath = getMetaPathInZK(tableName, esNameSpace);
        String tableString = null;
        try {
            tableString = Bytes.toString(this.zookeeper.getData(tablePath, true, null));
        } catch (KeeperException | InterruptedException e) {
        }
        Gson gson = new Gson();
        ESTable tableDef = gson.fromJson(tableString, ESTable.class);
        return tableDef;
    }

    /**
     * 
      * @Title: getTableDefWithName
      * @Description: TODO
      * @param tableName
      * @param nameSpace
      * @return
      * @throws KeeperException
      * @throws InterruptedException
     */
    public Table getTableDefWithName(String tableName, String nameSpace) throws KeeperException, InterruptedException {
        String tablePath = getMetaPathInZK(tableName, nameSpace);
        String tableString = Bytes.toString(this.zookeeper.getData(tablePath, true, null));
        Gson gson = new Gson();
        Table tableDef = null;
        if(esNameSpace.equals(nameSpace)){
            ESTable table = gson.fromJson(tableString, ESTable.class);
            tableDef = table;
        }else{
            HbaseTable table = gson.fromJson(tableString, HbaseTable.class);
            tableDef = table;
        }
        return tableDef;
    }

    /**
     * 
      * @Title: getTableDefine
      * @Description: TODO
      * @param table
      * @return
      * @throws IOException
      * @throws KeeperException
      * @throws InterruptedException
     */
    public List<Column> getTableDefine(HTable table) throws IOException, KeeperException, InterruptedException {
        return getFullTableDefine(table).getColumnList();
    }

    public String getMetaPathInZK(String tableName, String nameSpace) {
        if (tableName == null) {
            throw new IllegalArgumentException("null pointer");
        }
        String namespacePath = ZKUtil.joinZNode(this.metadataZNode, nameSpace);
        String tablePath = ZKUtil.joinZNode(namespacePath, tableName);
        return tablePath;
    }

    /**
     * 
      * @Title: updateTableDefine
      * @Description: TODO
      * @param table
      * @param columns
      * @throws IOException
      * @throws NoNodeException
      * @throws KeeperException
     * @throws InterruptedException 
     */
    public void updateTableDefine(HTable table, List<Column> columns) throws IOException, KeeperException, InterruptedException {
        if ((table == null) || (columns == null)) {
            throw new IllegalArgumentException("null pointer");
        }
        TableName tableName = table.getName();

        deleteTableDefine(tableName.getNameAsString(),hbaseNameSpace);
        setHbaseTableDefine(table, columns);

    }

    /**
     * 
      * @Title: deleteTableDefine
      * @Description: TODO
      * @param tableName
      * @param nameSpace
     * @throws InterruptedException 
      * @throws IOException
      * @throws KeeperException
     */
    public void deleteTableDefineInZookeeper(String tableName, String type) throws KeeperException, InterruptedException  {
        if("es".equals(type)){
            deleteTableDefine(tableName, esNameSpace);
        }else{
            deleteTableDefine(tableName,  hbaseNameSpace);
        }
    }
    
    /**
     * 
      * @Title: deleteTableDefine
      * @Description: TODO
      * @param tableName
      * @param nameSpace
      * @throws IOException
      * @throws KeeperException
     * @throws InterruptedException 
     */
    public void deleteTableDefine(String tableName, String nameSpace) throws KeeperException, InterruptedException {
        if (tableName == null) {
            throw new IllegalArgumentException("null pointer");
        }
        String tablePath = getMetaPathInZK(tableName, nameSpace);
        this.zookeeper.delete(tablePath, -1);
//        ZKUtil.deleteNode(zookeeper, tablePath);
    }

    /**
     * 
      * @Title: setTableDefine
      * @Description: TODO
      * @param table
      * @throws KeeperException
     * @throws InterruptedException 
     */
    public void setTableDefine(Table table) throws KeeperException, InterruptedException {
        if(table instanceof HbaseTable){
            String tableName = table.getName();
            String namespacePath = ZKUtil.joinZNode(this.metadataZNode, hbaseNameSpace);
            String tablePath = ZKUtil.joinZNode(namespacePath, tableName);
            Gson gson = new Gson();
            String data = gson.toJson(table);

            this.zookeeper.create(tablePath, Bytes.toBytes(data), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//            ZKUtil.createWithParents(this.zookeeper, tablePath, Bytes.toBytes(data));
        }else{
            setESTableDefine((ESTable)table);
        }
        
      
    }
    
    
    public static void main(String[] args) throws ZooKeeperConnectionException, KeeperException, IOException, ClassNotFoundException, ESHbaseException, InterruptedException {
    }
    
}
