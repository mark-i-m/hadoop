
package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;

public class Prefetcher {
    public static final Prefetcher PREFETCHER = new Prefetcher();

    private Prefetcher() {
        // TODO
    }

    // synchronized here means we always lock the whole prefetcher... maybe we
    // want to do something finer-grain.
    public synchronized int read(String filename, 
                    int offset, 
                    int length, 
                    char[] cbuf) 
        throws IOException 
    {
        return -1; // TODO
    }
}
