/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.core.data.manager.realtime;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.extractors.FieldExtractorFactory;
import com.linkedin.pinot.core.data.extractors.PlainFieldExtractor;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.query.MAggregationOperator;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaMessageDecoder;
import com.linkedin.pinot.core.realtime.impl.kafka.SimpleConsumerWrapper;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * TODO Document me!
 */
public class LLRealtimeSegmentDataManager extends SegmentDataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(LLRealtimeSegmentDataManager.class);

  private final LLCRealtimeSegmentZKMetadata _segmentZKMetadata;
  private final AbstractTableConfig _tableConfig;
  private final InstanceZKMetadata _instanceZKMetadata;
  private final RealtimeTableDataManager _realtimeTableDataManager;
  private final String _absolutePath;
  private final ReadMode _readMode;
  private final Schema _schema;
  private final ServerMetrics _serverMetrics;
  private final RealtimeSegmentImpl _realtimeSegment;
  private volatile boolean _stopConsuming;
  private Thread _indexingThread;

  public LLRealtimeSegmentDataManager(RealtimeSegmentZKMetadata segmentZKMetadata, AbstractTableConfig tableConfig,
      InstanceZKMetadata instanceZKMetadata, RealtimeTableDataManager realtimeTableDataManager, String absolutePath,
      ReadMode readMode, Schema schema, ServerMetrics serverMetrics) throws Exception {
    _segmentZKMetadata = (LLCRealtimeSegmentZKMetadata) segmentZKMetadata;
    _tableConfig = tableConfig;
    _instanceZKMetadata = instanceZKMetadata;
    _realtimeTableDataManager = realtimeTableDataManager;
    _absolutePath = absolutePath;
    _readMode = readMode;
    _schema = schema;
    _serverMetrics = serverMetrics;

    // Load configs
    // TODO Validate configs
    final String bootstrapNodes = _tableConfig.getIndexingConfig().getStreamConfigs()
        .get(CommonConstants.Helix.DataSource.STREAM_PREFIX + "." + CommonConstants.Helix.DataSource.Realtime.Kafka.KAFKA_BROKER_LIST);
    final String kafkaTopic = _tableConfig.getIndexingConfig().getStreamConfigs()
        .get(CommonConstants.Helix.DataSource.STREAM_PREFIX + "." + CommonConstants.Helix.DataSource.Realtime.Kafka.TOPIC_NAME);
    final LLCSegmentName segmentName = new LLCSegmentName(_segmentZKMetadata.getSegmentName());
    final int kafkaPartitionId = segmentName.getPartitionId();
    final String decoderClassName = _tableConfig.getIndexingConfig().getStreamConfigs()
        .get(CommonConstants.Helix.DataSource.STREAM_PREFIX + "." + CommonConstants.Helix.DataSource.Realtime.Kafka.DECODER_CLASS);

    // Start new realtime segment
    // HACK jfim hardcoded value for segment size
    _realtimeSegment = new RealtimeSegmentImpl(schema, 200000, tableConfig.getTableName(),
        segmentZKMetadata.getSegmentName(), kafkaTopic, serverMetrics);
    _realtimeSegment.setSegmentMetadata(segmentZKMetadata, schema);

    // Create message decoder
    final KafkaMessageDecoder messageDecoder = (KafkaMessageDecoder) Class.forName(decoderClassName).newInstance();
    messageDecoder.init(new HashMap<String, String>(), _schema, kafkaTopic);

    // Create field extractor
    final PlainFieldExtractor fieldExtractor = (PlainFieldExtractor) FieldExtractorFactory.getPlainFieldExtractor(schema);

    // Start indexing thread
    _indexingThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          // Create Kafka consumer
          SimpleConsumerWrapper consumerWrapper =
              SimpleConsumerWrapper.forPartitionConsumption(bootstrapNodes, kafkaTopic, kafkaPartitionId);

          // TODO Check for limit conditions (eg. full, stop consuming)
          boolean notFull = true;
          long currentOffset = _segmentZKMetadata.getStartOffset();

          while (notFull) {
            // Get a batch of messages from Kafka
            Iterable<MessageAndOffset> messagesAndOffsets =
                consumerWrapper.fetchMessages(currentOffset, Long.MAX_VALUE, 1000);

            // Index each message
            int batchSize = 0;
            for (MessageAndOffset messageAndOffset : messagesAndOffsets) {
              byte[] array = messageAndOffset.message().payload().array();
              int offset = messageAndOffset.message().payload().arrayOffset();
              int length = messageAndOffset.message().payloadSize();
              GenericRow row = messageDecoder.decode(array, offset, length);

              if (row != null) {
                row = fieldExtractor.transform(row);
                notFull = _realtimeSegment.index(row);
                batchSize++;
              } else {
                System.out.println("Dropped null row?");
              }
              currentOffset = messageAndOffset.nextOffset();
            }

            if (batchSize != 0) {
              System.out.println("Indexed " + batchSize + " messages from partition " + kafkaPartitionId + " offset " + currentOffset);
            } else {
              Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            }
          }
        } catch (Exception e) {
          LOGGER.error("Caught exception while indexing events", e);
        }
      }
    }, "Realtime indexing thread for " + _segmentZKMetadata.getSegmentName());
    _indexingThread.start();
  }

  @Override
  public IndexSegment getSegment() {
    return _realtimeSegment;
  }

  @Override
  public String getSegmentName() {
    return _realtimeSegment.getSegmentName();
  }

  @Override
  public void destroy() {
    // TODO
  }

  public void goOnlineFromConsuming() {
    // Download segment
    // Load segment
    // Atomically swap the segment
  }
}
