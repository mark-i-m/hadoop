
package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.mapred.PrefetchBuffer;

public class Prefetcher {
    /**
     * The single global instance of this object that everyone should use.
     */
    public static final Prefetcher PREFETCHER = new Prefetcher();

    /**
     * The prefetch buffer this prefetcher uses.
     *
     * All prefetches/reads the prefetcher does are issued to disk through this
     * object.
     */
    private final PrefetchBuffer buffer;

    private Prefetcher() {
        buffer = new PrefetchBuffer();
    }

    /**
     * Read the given section of the given file from this buffer into the given
     * buffer, filling the given buffer completely if there are enough bytes to
     * do so.
     *
     * @param filename the file to prefetch from
     * @param offset the offset into the file to prefetch
     * @param length the number of bytes to prefetch
     * @param buf the buffer into which to prefetch
     * @return the number of bytes read
     */
    public synchronized int read(String filename,
    // FIXME synchronized here means we always lock the whole prefetcher...
    // maybe we want to do something finer-grain.
                    long offset,
                    long length,
                    byte[] buf)
        throws IOException
    {
        // TODO
        buffer.prefetch(filename, offset, length);
        return buffer.read(filename, offset, buf);
    }
}
