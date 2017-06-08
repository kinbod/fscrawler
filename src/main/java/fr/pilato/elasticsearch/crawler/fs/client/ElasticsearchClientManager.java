/*
 * Licensed to David Pilato (the "Author") under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Author licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package fr.pilato.elasticsearch.crawler.fs.client;

import fr.pilato.elasticsearch.crawler.fs.meta.settings.FsSettings;
import fr.pilato.elasticsearch.crawler.fs.util.FsCrawlerUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;

import static fr.pilato.elasticsearch.crawler.fs.util.FsCrawlerUtil.INDEX_SETTINGS_FILE;
import static fr.pilato.elasticsearch.crawler.fs.util.FsCrawlerUtil.INDEX_TYPE_DOC;
import static fr.pilato.elasticsearch.crawler.fs.util.FsCrawlerUtil.INDEX_TYPE_FOLDER;
import static fr.pilato.elasticsearch.crawler.fs.util.FsCrawlerUtil.extractMajorVersionNumber;

public class ElasticsearchClientManager {
    private final Logger logger = LogManager.getLogger(ElasticsearchClientManager.class);
    private final Path config;
    private final FsSettings settings;

    private ElasticsearchClient client = null;
    private BulkProcessor bulkProcessor = null;

    public ElasticsearchClientManager(Path config, FsSettings settings) {
        this.config = config;
        this.settings = settings;
    }

    public ElasticsearchClient client() {
        if (client == null) {
            throw new RuntimeException("You must call start() before client()");
        }
        return client;
    }

    public BulkProcessor bulkProcessor() {
        if (bulkProcessor == null) {
            throw new RuntimeException("You must call start() before bulkProcessor()");
        }
        return bulkProcessor;
    }

    public void start() throws Exception {
        try {
            // Create an elasticsearch client
            client = new ElasticsearchClient(settings.getElasticsearch());
            // We set what will be elasticsearch behavior as it depends on the cluster version
            client.setElasticsearchBehavior();
        } catch (Exception e) {
            logger.warn("failed to create index [{}], disabling crawler...", settings.getElasticsearch().getIndex());
            throw e;
        }

        // Check that we don't try using an ingest pipeline with a non compatible version
        if (settings.getElasticsearch().getPipeline() != null && !client.isIngestSupported()) {
            throw new RuntimeException("You defined pipeline:" + settings.getElasticsearch().getPipeline() +
                    ", but your elasticsearch cluster does not support this feature.");
        }

        bulkProcessor = BulkProcessor.retryBulkProcessor(client, settings.getElasticsearch().getBulkSize(),
                settings.getElasticsearch().getFlushInterval(), settings.getElasticsearch().getPipeline());
    }

    public void createIndices(FsSettings settings) throws Exception {
        String elasticsearchVersion;
        Path jobMappingDir = config.resolve(settings.getName()).resolve("_mappings");

        // Let's read the current version of elasticsearch cluster
        String version = client.findVersion();
        logger.debug("FS crawler connected to an elasticsearch [{}] node.", version);

        elasticsearchVersion = extractMajorVersionNumber(version);

        // If needed, we create the new settings for this files index
        boolean pushMapping =
                settings.getFs().isAddAsInnerObject() == false || (!settings.getFs().isJsonSupport() && !settings.getFs().isXmlSupport());
        createIndex(jobMappingDir, elasticsearchVersion, INDEX_TYPE_DOC, pushMapping);

        // If needed, we create the new settings for this folder index
        pushMapping = settings.getFs().isIndexFolders();
        createIndex(jobMappingDir, elasticsearchVersion, INDEX_TYPE_FOLDER, pushMapping);
    }

    private void createIndex(Path jobMappingDir, String elasticsearchVersion, String deprecatedType, boolean pushMapping) throws Exception {
        try {
            // If needed, we create the new settings for this files index
            // Read index settings from resources. We try first to read specific settings file per type of index
            String indexSettings;
            try {
                indexSettings = FsCrawlerUtil.readJsonFile(jobMappingDir, config, elasticsearchVersion, INDEX_SETTINGS_FILE + "_" + deprecatedType);
            } catch (IllegalArgumentException e) {
                // TODO remove that. It's here only to keep some kind of backward compatibility
                indexSettings = FsCrawlerUtil.readJsonFile(jobMappingDir, config, elasticsearchVersion, INDEX_SETTINGS_FILE);
            }
            String mapping = null;

            // If needed, we create the new mapping for it
            if (pushMapping) {
                // Read file mapping from resources
                mapping = FsCrawlerUtil.readJsonFile(jobMappingDir, config, elasticsearchVersion, deprecatedType);
            }

            client.createIndex(settings.getElasticsearch().getIndex() + "_" + deprecatedType, true, indexSettings, mapping);
        } catch (Exception e) {
            logger.warn("failed to create index [{}], disabling crawler...", settings.getElasticsearch().getIndex());
            throw e;
        }
    }

    public void close() {
        logger.debug("Closing Elasticsearch client manager");
        if (bulkProcessor != null) {
            try {
                bulkProcessor.close();
            } catch (InterruptedException e) {
                logger.warn("Can not close bulk processor", e);
            }
        }
        if (client != null) {
            try {
                client.shutdown();
            } catch (IOException e) {
                logger.warn("Can not close elasticsearch client", e);
            }
        }
    }
}
