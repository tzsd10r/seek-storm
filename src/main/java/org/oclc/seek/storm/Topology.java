/****************************************************************************************************************
 * Copyright (c) 2015 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.storm;

import java.util.Properties;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.oclc.seek.storm.bolt.BoltBuilder;
// import org.oclc.seek.storm.bolt.MongodbBolt;
import org.oclc.seek.storm.bolt.SinkTypeBolt;
import org.oclc.seek.storm.bolt.SolrBolt;
import org.oclc.seek.storm.spout.SpoutBuilder;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.KafkaSpout;

/**
 * This is the main topology class.
 * All the spouts and bolts are linked together and is submitted on to the cluster
 */
public class Topology {

    private Properties configs;
    private BoltBuilder boltBuilder;
    private SpoutBuilder spoutBuilder;

    public static final String SOLR_STREAM = "solr-stream";
    public static final String HDFS_STREAM = "hdfs-stream";

    private static final String KAFKA_SPOUT_COUNT = "kafka.spout.count";
    private static final String KAFKA_SPOUT_ID = "kafka.spout";
    private static final String SINK_TYPE_BOLT_ID = "sink.type.bolt";
    private static final String SINK_BOLT_COUNT = "sink.bolt.count";

    private static final String SOLR_BOLT_ID = "solr.bolt";
    private static final String SOLR_BOLT_COUNT = "solr.bolt.count";
    private static final String SOLR_ZOOKEEPER_HOSTS = "solr.zookeeper.hosts";

    private static final String HDFS_BOLT_ID = "hdfs.bolt";
    private static final String HDFS_BOLT_COUNT = "hdfs.bolt.count";

    private static final String TOPOLOGY_NAME = "topology";

    public Topology(final String configFile) throws Exception {
        configs = new Properties();
        try {
            configs.load(ClassLoader.getSystemResourceAsStream(configFile));
            // configs.load(Topology.class.getResourceAsStream("config.properties"));
            boltBuilder = new BoltBuilder(configs);
            spoutBuilder = new SpoutBuilder(configs);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(0);
        }
    }

    private void buildAndSubmit() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpout kafkaSpout = spoutBuilder.buildKafkaSpout();
        SinkTypeBolt sinkTypeBolt = boltBuilder.buildSinkTypeBolt();
        SolrBolt solrBolt = boltBuilder.buildSolrBolt();
        HdfsBolt hdfsBolt = boltBuilder.buildHdfsBolt();

        // set the kafkaSpout to topology
        // parallelism-hint for kafkaSpout - defines number of executors/threads to be spawn per container
        int kafkaSpoutCount = Integer.parseInt(configs.getProperty(KAFKA_SPOUT_COUNT));
        builder.setSpout(configs.getProperty(KAFKA_SPOUT_ID), kafkaSpout, kafkaSpoutCount);

        // set the sinktype bolt
        int sinkBoltCount = Integer.parseInt(configs.getProperty(SINK_BOLT_COUNT));
        builder.setBolt(configs.getProperty(SINK_TYPE_BOLT_ID), sinkTypeBolt, sinkBoltCount).shuffleGrouping(
            configs.getProperty(KAFKA_SPOUT_ID));

        // set the solr bolt
        int solrBoltCount = Integer.parseInt(configs.getProperty(SOLR_BOLT_COUNT));
        builder.setBolt(configs.getProperty(SOLR_BOLT_ID), solrBolt, solrBoltCount).shuffleGrouping(
            configs.getProperty(SINK_TYPE_BOLT_ID), SOLR_STREAM);

        // set the hdfs bolt
        int hdfsBoltCount = Integer.parseInt(configs.getProperty(HDFS_BOLT_COUNT));
        builder.setBolt(configs.getProperty(HDFS_BOLT_ID), hdfsBolt, hdfsBoltCount).shuffleGrouping(
            configs.getProperty(SINK_TYPE_BOLT_ID), HDFS_STREAM);

        Config conf = new Config();
        conf.put("solr.zookeeper.hosts", configs.getProperty(SOLR_ZOOKEEPER_HOSTS));
        String topologyName = configs.getProperty(TOPOLOGY_NAME);
        // Defines how many worker processes have to be created for the topology in the cluster.
        conf.setNumWorkers(1);
        StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
    }

    public static void main(final String[] args) throws Exception {
        String configFile;
        if (args.length == 0) {
            configFile = "config.properties";
            System.out.println("Missing input : config file location, using default: " + configFile);
        } else {
            configFile = args[0];
        }

        Topology topology = new Topology(configFile);
        topology.buildAndSubmit();
    }
}
