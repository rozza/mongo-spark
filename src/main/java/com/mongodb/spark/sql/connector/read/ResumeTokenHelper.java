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
package com.mongodb.spark.sql.connector.read;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.jetbrains.annotations.Nullable;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

final class ResumeTokenHelper {

  static final BsonTimestamp MIN_TIMESTAMP = new BsonTimestamp(Long.MIN_VALUE);

  /**
   * @param resumeToken the optional resumeToken
   * @return the timestamp from the resume token or MIN_TIMESTAMP
   */
  static BsonTimestamp getTimestampFromResumeToken(@Nullable final BsonDocument resumeToken) {
    if (resumeToken == null) {
      return MIN_TIMESTAMP;
    } else if (!resumeToken.containsKey("_data")) {
      return MIN_TIMESTAMP;
    }
    return getTimestamp(resumeToken);
  }

  /**
   * The resume token has the following structure:
   *
   * <p>1. It's a document containing a single field named "_data" whose value is a string
   *
   * <p>2. The string is hex-encoded
   *
   * <p>3. The first byte is the "canonical type" for a BSON timestamp, encoded as an unsigned byte.
   * It should always equal 130.
   *
   * <p>4. The next 8 bytes are the BSON timestamp representing the operation time, encoded as an
   * unsigned long value with big endian byte order (unlike BSON itself, which is little endian).
   *
   * <p>The {@link BsonTimestamp} class contains thelogic for pulling out the seconds since the
   * epoch from that value. See <a href="http://bsonspec.org">http://bsonspec.org</a> for details.
   *
   * @param resumeToken a BsonDocument containing the resume token
   * @return the operation time contained within the resume token
   */
  protected static BsonTimestamp getTimestamp(final BsonDocument resumeToken) {
    String hexString = resumeToken.getString("_data").getValue();

    int len = hexString.length();
    byte[] bytes = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      bytes[i / 2] =
          (byte)
              ((Character.digit(hexString.charAt(i), 16) << 4)
                  + Character.digit(hexString.charAt(i + 1), 16));
    }

    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);

    // cast to an int then remove the sign bit to get the unsigned value
    int canonicalType = ((int) byteBuffer.get()) & 0xff;
    if (canonicalType != 130) {
      throw new IllegalArgumentException(
          "Expected canonical type equal to 130, but found " + canonicalType);
    }

    long timestampAsLong = byteBuffer.asLongBuffer().get();
    return new BsonTimestamp(timestampAsLong);
  }

  private ResumeTokenHelper() {}
}
