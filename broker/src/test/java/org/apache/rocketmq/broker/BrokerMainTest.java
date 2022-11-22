package org.apache.rocketmq.broker;

import java.io.File;
import java.util.UUID;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;

/**
 * @author ligm
 * @version 1.0
 * @date 2022/11/21 15:06
 */
public class BrokerMainTest {

    public static void main(String[] args) throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setBrokerRole(BrokerRole.ASYNC_MASTER);
        messageStoreConfig.setDeleteWhen("04");

        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerId(0L);
        brokerConfig.setBrokerName("broker-a");
        brokerConfig.setBrokerClusterName("default-cluster-a");
        brokerConfig.setNamesrvAddr("127.0.0.1:9876");

        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(10911);

        BrokerController brokerController = new BrokerController(brokerConfig, nettyServerConfig, new NettyClientConfig(), messageStoreConfig);
        brokerController.initialize();
        brokerController.start();
    }
}
