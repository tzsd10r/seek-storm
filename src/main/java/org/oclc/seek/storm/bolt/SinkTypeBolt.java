/****************************************************************************************************************
 * Copyright (c) 2015 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.storm.bolt;

import java.util.Map;

import org.oclc.seek.storm.Topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This class parses the incoming messages and decided which bolt the message has to be passed on to
 * There are two cases in this example, first if of solr type and second is of hdfs type.
 */
public class SinkTypeBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;

    public void execute(final Tuple tuple) {
        String value = tuple.getString(0);
        System.out.println("Received in SinkType bolt : " + value);
        int index = value.indexOf(" ");
        if (index == -1) {
            return;
        }
        String type = value.substring(0, index);
        System.out.println("Type : " + type);
        value = value.substring(index);
        if (type.equals("solr")) {
            collector.emit(Topology.SOLR_STREAM, new Values(type, value));
            System.out.println("Emitted : " + value);
        } else if (type.equals("hdfs")) {
            collector.emit(Topology.HDFS_STREAM, new Values(type, value));
            System.out.println("Emitted : " + value);
        }

        collector.ack(tuple);
    }

    public void prepare(final Map conf, final TopologyContext context, final OutputCollector collector) {
        this.collector = collector;

    }

    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declareStream(Topology.SOLR_STREAM, new Fields("sinkType", "content"));
        declarer.declareStream(Topology.HDFS_STREAM, new Fields("sinkType", "content"));
    }

}
