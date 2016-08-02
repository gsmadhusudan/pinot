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
package com.linkedin.pinot.core.segment.index.loader;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.segment.IndexLoadingConfigMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.io.reader.SingleColumnMultiValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitMultiValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.creator.impl.inv.OffHeapBitmapInvertedIndexCreator;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.segment.store.ColumnIndexType;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Use mmap to load the segment and perform all pre-processing steps. (This can be slow)
 * Pre-processing steps include:
 * - Generate inverted index.
 * - Generate default value for new added columns.
 */
public class SegmentPreProcessor implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPreProcessor.class);

  private final File indexDir;
  private final SegmentMetadataImpl segmentMetadata;
  private final String segmentName;
  private final SegmentVersion segmentVersion;
  private final IndexLoadingConfigMetadata indexConfig;
  private final Schema schema;
  private final SegmentDirectory segmentDirectory;

  SegmentPreProcessor(File indexDir, IndexLoadingConfigMetadata indexConfig, Schema schema)
      throws Exception {
    Preconditions.checkNotNull(indexDir);
    Preconditions.checkState(indexDir.exists(), "Segment directory: {} does not exist", indexDir);
    Preconditions.checkState(indexDir.isDirectory(), "Segment path: {} is not a directory", indexDir);

    this.indexDir = indexDir;
    segmentMetadata = new SegmentMetadataImpl(indexDir);
    segmentName = segmentMetadata.getName();
    segmentVersion = SegmentVersion.valueOf(segmentMetadata.getVersion());
    this.indexConfig = indexConfig;
    this.schema = schema;
    // Always use mmap to load the segment because it is safest and performs well without impact from -Xmx params.
    // This is not the final load of the segment.
    segmentDirectory = SegmentDirectory.createFromLocalFS(indexDir, segmentMetadata, ReadMode.mmap);
  }

  public void process()
      throws Exception {
    SegmentDirectory.Writer segmentWriter = null;
    try {
      segmentWriter = segmentDirectory.createWriter();
      createInvertedIndices(segmentWriter);

      // This step may modify the segment metadata. When adding new steps after this, reload the segment metadata.
      DefaultColumnHandlerFactory.getDefaultColumnHandler(indexDir, schema, segmentMetadata, segmentWriter)
          .updateDefaultColumns();
    } finally {
      if (segmentWriter != null) {
        segmentWriter.saveAndClose();
      }
    }
  }

  private void createInvertedIndices(SegmentDirectory.Writer segmentWriter)
      throws IOException {
    Set<String> invertedIndexColumns = getInvertedIndexColumns();

    for (String column : invertedIndexColumns) {
      createInvertedIndexForColumn(segmentWriter, segmentMetadata.getColumnMetadataFor(column));
    }
  }

  private Set<String> getInvertedIndexColumns() {
    Set<String> invertedIndexColumns = new HashSet<>();
    if (indexConfig == null) {
      return invertedIndexColumns;
    }

    Set<String> invertedIndexColumnsFromConfig = indexConfig.getLoadingInvertedIndexColumns();
    for (String column : invertedIndexColumnsFromConfig) {
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null && !columnMetadata.isSorted()) {
        invertedIndexColumns.add(column);
      }
    }

    return invertedIndexColumns;
  }

  private void createInvertedIndexForColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws IOException {
    String column = columnMetadata.getColumnName();
    File inProgress = new File(indexDir, column + ".inv.inprogress");
    File invertedIndexFile = new File(indexDir, column + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.

      if (segmentWriter.hasIndexFor(column, ColumnIndexType.INVERTED_INDEX)) {
        // Skip creating inverted index if already exists.

        LOGGER.info("Found inverted index for segment: {}, column: {}", segmentName, column);
        return;
      }

      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.

      // Remove inverted index if exists.
      // For v1 and v2, it's the actual inverted index. For v3, it's the temporary inverted index.
      FileUtils.deleteQuietly(invertedIndexFile);
    }

    // Create new inverted index for the column.
    LOGGER.info("Creating new inverted index for segment: {}, column: {}", segmentName, column);
    int totalDocs = columnMetadata.getTotalDocs();
    OffHeapBitmapInvertedIndexCreator creator =
        new OffHeapBitmapInvertedIndexCreator(indexDir, columnMetadata.getCardinality(), totalDocs,
            columnMetadata.getTotalNumberOfEntries(), columnMetadata.toFieldSpec());

    try (DataFileReader fwdIndex = getForwardIndexReader(columnMetadata, segmentWriter)) {
      if (columnMetadata.isSingleValue()) {
        // Single-value column.

        FixedBitSingleValueReader svFwdIndex = (FixedBitSingleValueReader) fwdIndex;
        for (int i = 0; i < totalDocs; i++) {
          creator.add(i, svFwdIndex.getInt(i));
        }
      } else {
        // Multi-value column.

        SingleColumnMultiValueReader mvFwdIndex = (SingleColumnMultiValueReader) fwdIndex;
        int[] dictIds = new int[columnMetadata.getMaxNumberOfMultiValues()];
        for (int i = 0; i < totalDocs; i++) {
          int len = mvFwdIndex.getIntArray(i, dictIds);
          creator.add(i, dictIds, len);
        }
      }
    }

    creator.seal();

    // For v3, write the generated inverted index file into the single file and remove it.
    if (segmentVersion == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(segmentWriter, column, invertedIndexFile, ColumnIndexType.INVERTED_INDEX);
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created inverted index for segment: {}, column: {}", segmentName, column);
  }

  private DataFileReader getForwardIndexReader(ColumnMetadata columnMetadata, SegmentDirectory.Writer segmentWriter)
      throws IOException {
    PinotDataBuffer buffer = segmentWriter.getIndexFor(columnMetadata.getColumnName(), ColumnIndexType.FORWARD_INDEX);
    if (columnMetadata.isSingleValue()) {
      return new FixedBitSingleValueReader(buffer, columnMetadata.getTotalDocs(), columnMetadata.getBitsPerElement(),
          columnMetadata.hasNulls());
    } else {
      return new FixedBitMultiValueReader(buffer, columnMetadata.getTotalDocs(),
          columnMetadata.getTotalNumberOfEntries(), columnMetadata.getBitsPerElement(), false);
    }
  }

  @Override
  public void close()
      throws Exception {
    segmentDirectory.close();
  }
}
