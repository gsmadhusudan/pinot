/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.aggregation;

/**
 * Interface for ResultHolder to hold the result of aggregation.
 *
 */
public interface AggregationResultHolder {

  /**
   * Set the 'primitive double' aggregation result.
   * @param value
   */
  void setValue(double value);

  /**
   * Set the aggregation result value.
   * @param value
   */
  void setValue(Object value);

  /**
   * Returns the 'primitive double' aggregation result.
   * @return
   */
  double getDoubleResult();

  /**
   * Returns the result of aggregation.
   * @return
   */
  Object getResult();
}