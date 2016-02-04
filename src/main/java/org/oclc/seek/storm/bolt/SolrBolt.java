/****************************************************************************************************************
 * Copyright (c) 2015 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.storm.bolt;

import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * This class is used for ingesting data into SOLR
 */
public class SolrBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private SolrClient solrClient;
    private String solrAddress;

    /**
     * @param solrAddress url that is used to connect to solr
     */
    public SolrBolt(final String solrAddress) {
        this.solrAddress = solrAddress;

    }

    public void prepare(final Map conf, final TopologyContext context, final OutputCollector collector) {
        this.collector = collector;
        solrClient = new HttpSolrClient(solrAddress);
    }

    public void execute(final Tuple input) {

        SolrInputDocument document = getSolrInputDocumentForInput(input);
        try {
            solrClient.add(document);
            solrClient.commit();
            collector.ack(input);
        } catch (Exception e) {

        }

    }

    /**
     * Converts the tuple into SOLR document.
     * Input will have the content in the field named "content" ( this is set by the SinkTypeBolt )
     * It is assumed that the content will be of the format fieldName1:Value1 fieldName2:Value2 ..
     *
     * @param input
     * @return
     */
    public SolrInputDocument getSolrInputDocumentForInput(final Tuple input) {
        String content = (String) input.getValueByField("content");
        String[] parts = content.trim().split(" ");
        System.out.println("Received in SOLR bolt " + content);
        SolrInputDocument document = new SolrInputDocument();
        try {
            for (String part : parts) {
                String[] subParts = part.split(":");
                String fieldName = subParts[0];
                String value = subParts[1];
                document.addField(fieldName, value);
            }
        } catch (Exception e) {

        }
        return document;
    }

    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
    }

}
