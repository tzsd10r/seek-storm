/****************************************************************************************************************
 * Copyright (c) 2015 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.storm.bolt;

import java.util.Properties;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

/**
 * This class is used for building bolts
 */
public class BoltBuilder {

    private Properties configs = null;
    private static final String SOLR_SERVER = "solr.url";
    private static final String SOLR_COLLECTION = "solr.collection";

    private static final String HDFS_FOLDER = "hdfs.folder";
    private static final String HDFS_PORT = "hdfs.port";
    private static final String HDFS_HOST = "hdfs.host";

    public BoltBuilder(final Properties configs) {
        this.configs = configs;
    }

    public SinkTypeBolt buildSinkTypeBolt() {
        return new SinkTypeBolt();
    }

    public SolrBolt buildSolrBolt() {
        String solrServerUlr = configs.getProperty(SOLR_SERVER);
        String collection = configs.getProperty(SOLR_COLLECTION);
        SolrBolt bolt = new SolrBolt(solrServerUlr + collection);
        return bolt;
    }

    public HdfsBolt buildHdfsBolt() {
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");
        SyncPolicy syncPolicy = new CountSyncPolicy(1);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(configs.getProperty(HDFS_FOLDER));
        String port = configs.getProperty(HDFS_PORT);
        String host = configs.getProperty(HDFS_HOST);
        HdfsBolt bolt = new HdfsBolt().withFsUrl(host).withFileNameFormat(fileNameFormat)
            .withRecordFormat(format).withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);
        return bolt;
    }

}
