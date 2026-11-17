/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package opennlp.tools.util.model;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipInputStream;

import opennlp.tools.util.InvalidFormatException;
import opennlp.tools.util.ResourceLimits;

/**
 * A {@link ZipInputStream} that fails once the bytes inflated from the archive exceed
 * the bytes read from it by more than {@link ResourceLimits#MAX_COMPRESSION_RATIO}.
 * Archives that inflate to less than {@value #UNCHECKED_BYTES} bytes are not checked.
 */
final class BoundedZipInputStream extends ZipInputStream {

  static final long UNCHECKED_BYTES = 16L * 1024 * 1024;

  private final CountingInputStream archive;
  private long inflated;

  BoundedZipInputStream(InputStream in) {
    this(new CountingInputStream(in));
  }

  private BoundedZipInputStream(CountingInputStream archive) {
    super(archive);
    this.archive = archive;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    final int n = super.read(b, off, len);
    if (n > 0) {
      inflated += n;
      if (inflated > UNCHECKED_BYTES
          && inflated > archive.count * ResourceLimits.MAX_COMPRESSION_RATIO) {
        throw new InvalidFormatException("Model package exceeds the maximum compression ratio of "
            + ResourceLimits.MAX_COMPRESSION_RATIO + " (" + archive.count + " bytes read, "
            + inflated + " bytes inflated). The limit can be raised via -D"
            + ResourceLimits.MAX_COMPRESSION_RATIO_PROPERTY + "=<n>.");
      }
    }
    return n;
  }

  private static final class CountingInputStream extends FilterInputStream {

    private long count;

    private CountingInputStream(InputStream in) {
      super(in);
    }

    @Override
    public int read() throws IOException {
      final int b = super.read();
      if (b != -1) {
        count++;
      }
      return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      final int n = super.read(b, off, len);
      if (n > 0) {
        count += n;
      }
      return n;
    }

    @Override
    public long skip(long n) throws IOException {
      final long skipped = super.skip(n);
      count += skipped;
      return skipped;
    }
  }
}
