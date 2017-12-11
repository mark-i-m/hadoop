
package org.apache.hadoop.mapred;

/**
 * An interface that all prefetch policies must implement.
 */
public interface PrefetchPolicy {
    /**
     * Represents a single Prefetch returned by the policy
     */
    public class Prefetch {
        public final String filename;
        public final long offset;
        public final long length;

        public Prefetch(String filename, long offset, long length) {
            this.filename = filename;
            this.offset = offset;
            this.length = length;
        }

        @Override
        public String toString() {
            return "Prefetch { file=" + filename +
                   ", off=" + offset +
                   ", len=" + length +
                   " }";
        }
    }

    /**
     * Make a policy decision based on the current request and memory usage.
     *
     * @param filename the filename of the current requests
     * @param offset the offset into `filename` of the current request
     * @param length the number of bytes of the current request
     * @param memUsage the number of bytes currently in use by the buffer
     * @param memBound the total number of bytes the predictor is allowed to
     * use. In other words, memBound - memUsage is the remaining amount of
     * memory.
     * @param reducerId the ID of the reducer reading (used for prefetching)
     * @return a prediction for the prefetcher or null if the predictor should
     * do nothing.
     */
    public Prefetch next(String filename,
                         long offset,
                         long length,
                         long memUsage,
                         long memBound,
                         int reducerId);
}
