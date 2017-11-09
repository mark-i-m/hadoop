
package org.apache.hadoop.mapred;

import java.io.File;

import org.apache.hadoop.mapred.Prefetcher;

public class PrefetchedFile extends File {
    public PrefetchedFile(String filename) {
        super(filename);
    }
}
