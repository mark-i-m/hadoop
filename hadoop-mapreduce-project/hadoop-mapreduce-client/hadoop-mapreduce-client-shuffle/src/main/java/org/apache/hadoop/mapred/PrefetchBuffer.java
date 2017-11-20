
package org.apache.hadoop.mapred;

import java.io.RandomAccessFile;
import java.io.FileNotFoundException;
import java.io.IOException;

public class PrefetchBuffer {
    /**
     * Prefetch the given section of the given file into this buffer.
     *
     * A call to `prefetch` must precede every call to `read`.
     *
     * @param filename the file to prefetch from
     * @param offset the offset into the file to prefetch
     * @param length the number of bytes to prefetch
     * @throws FileNotFoundException if the requested file does not exist
     * @throws IOException if there is an error reading the requested file
     */
    public void prefetch(
            String filename,
            long offset,
            long length)
        throws FileNotFoundException, IOException
    {
        // TODO
    }

    /**
     * Read the given section of the given file from this buffer into the given
     * buffer, filling the given buffer completely if there are enough bytes to
     * do so.
     *
     * A call to `prefetch` must precede every call to `read`.
     *
     * @param filename the file to prefetch from
     * @param offset the offset into the file to prefetch
     * @param length the number of bytes to prefetch
     * @param buf the buffer into which to prefetch
     * @return the number of bytes read
     * @throws IOException if there is an error reading the requested file
     */
    public int read(
            String filename,
            long offset,
            byte[] buf)
        throws IOException
    {
        // TODO: for now we just open a file and read from it...
        RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(filename, "r");
        } catch(FileNotFoundException e) {
            throw new IOException("Unable to open file in dummy impl of read" +
                    filename);
        }
        raf.seek(offset);
        raf.readFully(buf);
        return buf.length;
    }
}
