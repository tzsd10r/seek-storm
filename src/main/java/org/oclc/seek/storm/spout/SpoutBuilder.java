/****************************************************************************************************************
 * Copyright (c) 2015 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.storm.spout;

import java.util.Properties;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.spout.SchemeAsMultiScheme;

/**
 *
 */
public class SpoutBuilder {
    private static final String KAFKA_ZOOKEEPER_HOSTS = "kafka.zookeeper.hosts";
    private static final String KAFKA_TOPIC = "kafka.topic";
    private static final String KAFKA_ZKROOT = "kafka.zkRoot";
    private static final String KAFKA_CONSUMERGROUP = "kafka.consumer.group";

    private Properties configs = null;

    public SpoutBuilder(final Properties configs) {
        this.configs = configs;
    }

    public KafkaSpout buildKafkaSpout() {
        BrokerHosts hosts = new ZkHosts(configs.getProperty(KAFKA_ZOOKEEPER_HOSTS));
        String topic = configs.getProperty(KAFKA_TOPIC);
        // the root path in Zookeeper for the spout to store the consumer offsets
        String zkRoot = configs.getProperty(KAFKA_ZKROOT);
        String groupId = configs.getProperty(KAFKA_CONSUMERGROUP);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, groupId);

        // ArrayList<String> list = new ArrayList<String>();
        // list.add("ilabhddb03dxdu.dev.oclc.org");
        // list.add("ilabhddb04dxdu.dev.oclc.org");
        // list.add("ilabhddb05dxdu.dev.oclc.org");
        // spoutConfig.zkServers = list;
        // spoutConfig.zkPort = new Integer(9011);
        // spoutConfig.zkRoot = "/brokers";

        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        return kafkaSpout;
    }
}
