package com.ning.dw.es;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.ning.dw.tools.ConfigReader;




/**
 * Es连接类
 * 
 * @ClassName: EsClient
 * @Description: Es连接类
 * @author netposa
 * @date 2016年5月17日 下午1:50:26
 *
 */
public class EsClient {

    public static TransportClient client = null;

    private static final Log LOG = LogFactory.getLog(EsClient.class);
    
    private EsClient() {
        
    }

    /**
     * 获取连接
     * 
     * @Title: getInstance
     * @Description: TODO
     * @return
     * @throws UnknownHostException 
     */
    public static TransportClient getInstance() throws Exception {
        
        if (client == null) {
          //2.3.1版本
            Settings settings = Settings.settingsBuilder().put("cluster.name", ConfigReader.getPropValue("cluster_name")).put("client.transport.sniff", true).build();
        //1.7.1版本
//       
//
//                // client.transport.sniff=true
//
//                // 客户端嗅探整个集群的状态，把集群中其它机器的ip地址自动添加到客户端中，并且自动发现新加入集群的机器
//                .put("client.transport.sniff", true).put("client", true)// 仅作为客户端连接
//                .put("data", false).put("cluster.name", ConfigReader.getPropValue("cluster_name"))// 集群名称
//                .build();
        //2.3.1版本
            client = TransportClient.builder().settings(settings).build();
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ConfigReader.getPropValue("address")), Integer.parseInt(ConfigReader.getPropValue("port"))));
        //1.7.1版本
//        client = new TransportClient(settings)
//                .addTransportAddress(new InetSocketTransportAddress(ConfigReader.getPropValue("address"), Integer.parseInt(ConfigReader.getPropValue("port"))))//TCP 连接地址
//                ;
            
        }

        return client;
    }

    public static void main(String[] args) throws Exception {
//        EsClient.getInstance();
    }
}
