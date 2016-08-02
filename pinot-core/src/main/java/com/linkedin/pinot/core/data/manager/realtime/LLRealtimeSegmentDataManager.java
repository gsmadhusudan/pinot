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

import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


/**
 * TODO Document me!
 */
public class LLRealtimeSegmentDataManager extends SegmentDataManager {
  public LLRealtimeSegmentDataManager() {
    // Load segment config
    // Create Kafka consumer
    // Start indexing thread
  }

  @Override
  public IndexSegment getSegment() {
    return null;
  }

  @Override
  public String getSegmentName() {
    return null;
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
