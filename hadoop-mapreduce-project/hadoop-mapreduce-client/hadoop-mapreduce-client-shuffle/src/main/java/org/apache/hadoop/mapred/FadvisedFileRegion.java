/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.io.ReadaheadPool.ReadaheadRequest;
import org.apache.hadoop.io.nativeio.NativeIO;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;

import org.jboss.netty.channel.DefaultFileRegion;

import com.google.common.annotations.VisibleForTesting;

public class FadvisedFileRegion extends DefaultFileRegion {

  private static final Log LOG = LogFactory.getLog(FadvisedFileRegion.class);

  private final FileDescriptor fd;
  private final String identifier;
  private final long count;
  private final long position;
  private final int shuffleBufferSize;
  private final boolean shuffleTransferToAllowed;
  private final RandomAccessFile file;

  private ReadaheadRequest readaheadRequest;

  public FadvisedFileRegion(RandomAccessFile file, long position, long count,
      boolean manageOsCache, int readaheadLength, ReadaheadPool readaheadPool,
      String identifier, int shuffleBufferSize,
      boolean shuffleTransferToAllowed) throws IOException {
    super(file.getChannel(), position, count);
    this.fd = file.getFD();
    this.identifier = identifier;
    this.file = file;
    this.count = count;
    this.position = position;
    this.shuffleBufferSize = shuffleBufferSize;
    this.shuffleTransferToAllowed = shuffleTransferToAllowed;
  }

  @Override
  public long transferTo(WritableByteChannel target, long position)
      throws IOException {

    // go to the right place in the file
    this.file.seek(position);

    // ask the file for bytes in chunks of no more than shuffleBufferSize
    ByteBuffer byteBuffer = ByteBuffer.allocate(this.shuffleBufferSize);
    long remainingBytes = this.count - position;

    while (remainingBytes > 0) {
      // read into the buffer, and mark what part of the buffer has data (as
      // opposed to junk)
      int bytesRead = this.file.read(byteBuffer.array());
      byteBuffer.position(0);
      byteBuffer.limit(Math.min(bytesRead, (int)remainingBytes));

      // Update remaining count
      remainingBytes -= bytesRead;

      // Sen the data we read
      while(byteBuffer.hasRemaining()) {
        target.write(byteBuffer);
      }

      byteBuffer.clear();
    }

    return this.count - position;
  }

  @Override
  public void releaseExternalResources() {
    super.releaseExternalResources();
  }

  /**
   * Call when the transfer completes successfully so we can advise the OS that
   * we don't need the region to be cached anymore.
   */
  public void transferSuccessful() {
  }
}
