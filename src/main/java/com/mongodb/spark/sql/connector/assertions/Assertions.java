/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.mongodb.spark.sql.connector.assertions;

import java.util.function.Supplier;

/** Assertions to validate inputs */
public interface Assertions {

  /**
   * Ensures the validity of state
   *
   * @param stateCheck the supplier of the state check
   * @param errorMessage the error message if the supplier fails
   * @throws IllegalStateException if the state check fails
   */
  static void ensureState(final Supplier<Boolean> stateCheck, final String errorMessage) {
    if (!stateCheck.get()) {
      throw new IllegalStateException(errorMessage);
    }
  }

  /**
   * Ensures the validity of arguments
   *
   * @param argumentCheck the supplier of the argument check
   * @param errorMessage the error message if the supplier fails
   * @throws IllegalArgumentException if the argument check fails
   */
  static void ensureArgument(final Supplier<Boolean> argumentCheck, final String errorMessage) {
    if (!argumentCheck.get()) {
      throw new IllegalArgumentException(errorMessage);
    }
  }
}
