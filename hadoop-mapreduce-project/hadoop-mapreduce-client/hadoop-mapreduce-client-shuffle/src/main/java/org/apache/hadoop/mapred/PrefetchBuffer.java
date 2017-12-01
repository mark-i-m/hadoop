
package org.apache.hadoop.mapred;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * PrefetchBuffer manages a read-only in-memory buffer for disk reads.
 *
 * Prefetched data is read-once. After it is read, it is discarded and
 * the memory freed for later allocations.
 */
public class PrefetchBuffer {
    /*
     * The prefetch buffer organizes its memory as a big ordered tree
     * of Regions, sorted by their start offset into a given file. i.e.
     * there is a tree of regions for each file being prefetched from.
     *
     * All regions are disjoint; there is no overlap. Thus, finding the
     * regions that contain data for some range of a file is efficient.
     *
     * Reading data from some range of the tree will cause that data to
     * be deallocated and the memory freed.
     *
     * Furthermore, all Regions's disk requests are inserted into a queue
     * (reqQueue), which the prefetching thread works through in order.
     */
    private HashMap<String, TreeSet<Region>> regions;
    private LinkedList<DiskReq> reqQueue;

    // A thread that just fetches stuff from disk. It sits around waiting
    // for requests in reqQueue and fullfilling such requests.
    private final PrefetchThread prefThread = new PrefetchThread();

    ///////////////////////////////////////////////////////////////////////////
    // Constructor
    ///////////////////////////////////////////////////////////////////////////

    public PrefetchBuffer() {
        prefThread.setDaemon(true);
        prefThread.setName("PrefetchThread");
        prefThread.start();
    }

    ///////////////////////////////////////////////////////////////////////////
    // Public API
    //
    // The public API of the PrefetchBuffer allows a `prefetch` request to be
    // made. This is an asynchronous function call which queues the given region
    // of the given file to be read from disk.
    //
    // Subsequently, a call to `read` will return the data from a given file
    // and offset. A call to `read` must be preceded by a call to `prefetch`.
    // If the data requested has not yet been fetched from disk, then the call
    // to `read` will block until it is fetched. Otherwise, the data will be
    // read directly from the buffer (memory), and that memory freed.
    ///////////////////////////////////////////////////////////////////////////

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
     *
     * NOTE: this method grabs the monitor lock!
     */
    public synchronized void prefetch(
            String filename,
            long offset,
            long length)
        throws FileNotFoundException, IOException
    {
        // Find unrequested parts of this request
        ArrayList<Region> gaps = findGaps(filename, offset, length);

        // Request all gaps
        insertAndEnqueueAll(filename, gaps);
    }

    /**
     * Read the given section of the given file from this buffer into the given
     * buffer, filling the given buffer completely if there are enough bytes to
     * do so.
     *
     * A call to `prefetch` must precede every call to `read`.
     *
     * This call may block waiting for IO, but hopefully it usually won't :P
     *
     * @param filename the file to prefetch from
     * @param offset the offset into the file to prefetch
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
        // Get the regions for the file
        TreeSet<Region> fRegions = this.regions.get(filename);
        if (fRegions == null) {
            fRegions = new TreeSet<>();
            this.regions.put(filename, fRegions);
        }

        // Compute the number of bytes to read. This is the minimume of the number
        // of prefetched bytes and the length of the file.
        long length = Math.min(fRegions.last().getAfter(), buf.length);

        // Get a sorted set of all regions covered by the given range of the file
        ArrayList<Region> covered = getRange(filename, offset, offset + length);

        long soFar = 0;
        for (Region r : covered) {
            r.readToBuffer(buf, soFar);
            soFar += r.getLength();
        }

        // Deallocate the Regions we just read!
        fRegions.removeAll(covered);

        return (int) length;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Helpers and buffer management
    ///////////////////////////////////////////////////////////////////////////

    /**
     * This method is basically a wrapper around TreeSet.subSet that accepts
     * `long` instead of `Region`. It is a convenience method.
     */
    private static SortedSet<Region> getTreeSubSet(TreeSet<Region> tree,
                                                    String filename,
                                                    long start,
                                                    long end)
    {
        // Create a couple of dummy regions so we can call subSet
        Region dummyStart = new Region(filename, start, 0, null, 0);
        Region dummyEnd = new Region(filename, end, 0, null, 0);

        return tree.subSet(dummyStart, dummyEnd);
    }

    /**
     * Finds all previously unrequested Regions in the given range of the file,
     * creates new Regions for them, _allocates_ memory to back IO for them, and
     * returns the new Regions _without inserting or enqueuing them_.
     *
     * NOTE: this call should be made while holding the monitor lock!
     */
    private ArrayList<Region> findGaps(String filename, long offset, long length) {
        // Get the regions for the file
        TreeSet<Region> fRegions = this.regions.get(filename);
        if (fRegions == null) {
            fRegions = new TreeSet<>();
            this.regions.put(filename, fRegions);
        }

        // Get a sorted set of all regions covered by the given range of the file
        SortedSet<Region> covered = getTreeSubSet(fRegions,
                                                  filename,
                                                  offset,
                                                  offset + length);

        // Iterate over the `covered` set, keeping track of the most latest
        // offset into the desired range that we have "handled".  Starting with
        // the beginning of the desired range, each sub-range is covered by
        // either a Region from `covered` or a new Region which we will create.
        //
        // Moreover, keep track of the space required for all new Regions, so
        // we can allocate memory afterwards.
        ArrayList<Region> newRegions = new ArrayList<>();
        long requiredSpace = 0;
        long latestHandled = offset;

        for (Region r : covered) {
            long regionStart = r.getStart();

            // If there is space between the beginning of `r` and `offset`, we
            // need to create a new Region. However, since a buffer can only
            // contain so much data, we need to make sure we respect the maximum
            // size of any buffer and break up large Regions appropriately.
            if (regionStart > offset) {
                long spaceNeeded = offset - regionStart;
                long nextOffset = offset;

                while (spaceNeeded > 0) {
                    long newRLen = Math.min(spaceNeeded, MiniBuffer.BUFFER_SIZE);

                    newRegions.add(new Region(filename, nextOffset, newRLen));

                    spaceNeeded -= newRLen;
                    nextOffset += newRLen;
                }

                offset = r.getAfter();
            }
        }

        // allocate memory, setup disk IO requests, update Regions
        MiniBuffer.allocateRegions(filename, newRegions);

        // Done!
        return newRegions;
    }

    /**
     * Sorts, inserts, and enqueues the given list of regions.
     *
     * The sorting is done to minimize seeking.
     *
     * NOTE: this call should be made while holding the monitor lock!
     */
    private void insertAndEnqueueAll(String filename, ArrayList<Region> list) {
        // Sort the regions
        Collections.sort(list);

        if (list.size() == 0) {
            return;
        }

        // Insert into the tree.
        TreeSet<Region> fRegions = this.regions.get(filename);
        if (fRegions == null) {
            fRegions = new TreeSet<>();
            this.regions.put(filename, fRegions);
        }

        fRegions.addAll(list);

        // Collect the set of disk requests for the given list of regions and
        // enqueue them all.
        SortedSet<DiskReq> requests = new TreeSet<>();
        for (Region r : list) {
            requests.add(r.getDiskReq());
        }

        synchronized (this.reqQueue) {
            this.reqQueue.addAll(requests);
        }
    }

    /**
     * Returns a list of requested Regions _exactly_ covering the given the
     * given range of the given file, possibly breaking up the first and last
     * Region to do so.
     *
     * @throws IllegalStateException if the range contains portions of the file
     * which have never been previously requested.
     */
    private synchronized ArrayList<Region> getRange(String filename,
                                                    long offset,
                                                    long length)
    {
        // Get the regions for the file
        TreeSet<Region> fRegions = this.regions.get(filename);
        if (fRegions == null) {
            fRegions = new TreeSet<>();
            this.regions.put(filename, fRegions);
        }

        // Get a sorted set of all regions covered by the given range of the file
        SortedSet<Region> covered = getTreeSubSet(fRegions,
                                                  filename,
                                                  offset,
                                                  offset + length);

        // Make sure they are contiguous and cover the whole range
        long soFar = offset;
        for (Region r : covered) {
            if (offset >= r.getStart() && offset < r.getAfter()) {
                soFar = r.getAfter();
            } else {
                throw new IllegalStateException(
                        "Requested range (offset=" + offset +
                        ", length=" + length +
                        ") from file " + filename +
                        ", but offset " + soFar +
                        " was never prefetched!"
                        );
            }
        }

        // If needed break the first and last Regions so that we have a set of
        // regions that perfectly the requested range.
        Region first = covered.first();
        if (first.getStart() != offset) {
            Region secondHalf = first.splitAt(offset);
            fRegions.add(secondHalf);
            covered.remove(first);
        }

        Region last = covered.last();
        if (last.getAfter() != offset + length) {
            Region secondHalf = last.splitAt(offset + length);
            fRegions.add(secondHalf);
            covered.remove(secondHalf);
        }

        // Convert to a list and return
        ArrayList<Region> finalList = new ArrayList<>();
        finalList.addAll(covered);
        return finalList;
    }

    ///////////////////////////////////////////////////////////////////////////
    // The Prefetcher Thread
    ///////////////////////////////////////////////////////////////////////////

    private class PrefetchThread extends Thread {
        @Override
        public void run() {
            while (true) {
                DiskReq next;

                // Pop the queue head while locking the queue
                synchronized (reqQueue) {
                    next = reqQueue.removeFirst();
                }

                // If no requests, sleep until later
                if (reqQueue == null) {
                    try {
                        Thread.sleep(100); // 100ms
                    } catch (InterruptedException ie) {
                        // It is OK!
                    }
                    continue;
                }

                // Process the request
                RandomAccessFile raf;
                IOException thrown = null;
                int idx = 0;
                try {
                    // Open the file
                    raf = new RandomAccessFile(next.filename, "r");

                    // Seek to the required offset
                    raf.seek(next.offset);

                    // Read into the MiniBuffer
                    long remaining = next.length;
                    for (idx = 0; idx < next.buffers.size(); idx++) {
                        MiniBuffer mb = next.buffers.get(idx);

                        if (remaining > 0) {
                            byte[] buf = mb.getBuffer();
                            int len = Math.min(buf.length, (int)remaining);
                            raf.readFully(buf, 0, len);
                            remaining -= len;
                        } else {
                            thrown = new EOFException(
                                    "End of File while processing request " + next
                                    + " at offset " + raf.getFilePointer()
                                    + ". File has length " + raf.length());

                            break;
                        }
                    }

                    // Close
                    raf.close();
                } catch (FileNotFoundException e) {
                    thrown = new IOException(
                            "Unable to open file in read request for " +
                            next.filename + ". Error: " + e);
                } catch (IOException e) {
                    // Includes EOFException
                    thrown = e;
                } finally {
                    // All remaining buffers get the same exception
                    for (; idx < next.buffers.size(); idx++) {
                        next.buffers.get(idx).setThrown(thrown);
                    }

                    // Notify
                    for (MiniBuffer mb : next.buffers) {
                        mb.dataReady();
                    }
                }
            }
        }
    }
}

/**
 * A small buffer containing some prefetched data.
 *
 * A single buffer can be shared by multiple Regions disjointly.
 *
 * Consumers of a buffer should call `waitForData` before attempting to read.
 * This will block their thread until IO completes and the data is ready for
 * consumption.
 */
class MiniBuffer {
    public static final int BUFFER_SIZE = 1 << 20; // 1MB

    // The memory where the data is held once IO completes
    private byte[] data = new byte[BUFFER_SIZE];

    // A flag indicating that the data is ready
    private boolean ready = false;

    // If any exception is thrown during IO, it is stored here
    private IOException thrown = null;

    /**
     * Wait for the data in this buffer to become available.
     */
    private synchronized void waitForData() {
        while (!this.ready) {
            try {
                this.wait();
            } catch (InterruptedException ie) {
                // It is ok!
            }
        }
    }

    /**
     * Wake up all threads waiting for the data.
     */
    public synchronized void dataReady() {
        this.ready = true;
        this.notifyAll();
    }

    /**
     * Allocate buffers for the given regions, create disk IO requests, and
     * update the Regions to reflect allocations.
     *
     * This method tries to compute the most efficient allocation of
     * memory possible to reduce fragmentation, but it guarantees that
     * on return, all Regions will have an allocation.
     *
     * It also computes the smallest number of possible DiskReqs and associates
     * them with their corresponding Regions.
     *
     * @throws IllegalArgumentException if any of the Regions is too
     * large (size greater than BUFFER_SIZE).
     */
    public static void allocateRegions(String filename, ArrayList<Region> regions) {
        // No regions... no problems
        if (regions.size() == 0) {
            return;
        }

        // Will contain the sets of new DiskReqs and MiniBuffers.
        ArrayList<DiskReq> requests = new ArrayList<>();
        ArrayList<MiniBuffer> buffers = new ArrayList<>();

        // Process each region keeping track of whether it can fit in the
        // most recent DiskReq or MiniBuffer.
        ArrayList<Region> latestReq = new ArrayList<>();
        ArrayList<Region> latestBuf = new ArrayList<>();
        ArrayList<MiniBuffer> reqBuffers = new ArrayList<>();
        long latestBufLen = 0;

        for (Region r : regions) {
            // Does the region fit into the latest buffer?
            if (latestBuf.size() == 0 ||
                latestBufLen + r.getLength() <= MiniBuffer.BUFFER_SIZE)
            {
                latestBuf.add(r);
            } else {
                // End the latestBuf and start a new one
                MiniBuffer buffer = new MiniBuffer();

                long bufferOffset = 0;
                for (Region bufr : latestBuf) {
                    bufr.setBuffer(buffer, bufferOffset);
                    bufferOffset += bufr.getLength();
                }

                reqBuffers.add(buffer);

                latestBuf.clear();
                latestBufLen = 0;
            }

            // Is the beginning of a new request or does it continue an
            // existing one?
            if (latestReq.size() == 0 ||
                latestReq.get(latestReq.size()-1).getAfter() == r.getStart())
            {
                latestReq.add(r);
            } else {
                // End the latestReq and start a new one
                long start = latestReq.get(0).getStart();
                long end = latestReq.get(latestReq.size()-1).getAfter();
                long length = end - start;

                DiskReq request = new DiskReq(filename, start, length);

                for (Region reqr : latestReq) {
                    reqr.setRequest(request);
                }

                request.addBuffers(reqBuffers);

                reqBuffers.clear();
                latestReq.clear();
            }
        }
    }

    /**
     * Read the contents of this Region into the given buffer. This may block
     * waiting for IO.
     *
     * @param offset the offset into this MiniBuffer of the first byte to read.
     * @param buffer the buffer to read this Region into.
     * @param bufferOffset the offset from the beginning of `buffer` where the
     * data should be read.
     * @throws IOException if there was an IOException during IO.
     */
    public void readToBuffer(long offset, byte[] buffer, long bufferOffset)
        throws IOException
    {
        // Possibly block on IO
        waitForData();

        // If there was an exception, rethrow it.
        if (this.thrown != null) {
            throw this.thrown;
        }

        // Copy the relevant part of this buffer to the relevant part of the
        // other buffer.
        for (long i = 0; i < buffer.length; i++) {
            buffer[(int)(bufferOffset + i)] = this.data[(int)(offset + i)];
        }
    }

    /**
     * @return the internal buffer this MiniBuffer uses.
     */
    public byte[] getBuffer() {
        return this.data;
    }

    public void setThrown(IOException thrown) {
        this.thrown = thrown;
    }
}

/**
 * Represents a single disk IO request to the prefetcher thread.
 */
class DiskReq {
    // What to request
    public String filename;
    public long offset;
    public long length;

    // The buffers to read data into
    public ArrayList<MiniBuffer> buffers;

    public DiskReq(String filename, long offset, long length) {
        this.filename = filename;
        this.offset = offset;
        this.length = length;
        this.buffers = new ArrayList<>();
    }

    public void addBuffers(ArrayList<MiniBuffer> buffers) {
        this.buffers.addAll(buffers);
    }
}

/**
 * Represents a contiguous region of a file which has yet to be either
 * prefetched or read from disk.
 *
 * Regions are tracked by the buffer in an ordered tree structure, so they need
 * be Comparable. Regions are ordered based on their start offset.
 */
class Region implements Comparable<Region> {
    // The location and length of the Region
    private String filename;
    private long offset;
    private long length;

    // The buffer where this Region's data is prefetched
    private MiniBuffer buffer;

    // The offset into `buffer` where this Region's data starts. i.e. index
    // `bufferOffset` into `buffer` corresponds with offset `offset` into the
    // given file.
    private long bufferOffset;

    // The DiskReq fetching data for this Region
    private DiskReq request;

    /**
     * Create a new Region with the given location, length, buffer, and
     * bufferOffset.
     */
    public Region(String filename,
                  long offset,
                  long length,
                  MiniBuffer buffer,
                  long bufferOffset)
    {
        this.filename = filename;
        this.offset = offset;
        this.length = length;
        this.buffer = buffer;
        this.bufferOffset = bufferOffset;
    }

    /**
     * Create a new Region with the given location, length, buffer, and
     * bufferOffset.
     */
    public Region(String filename,
                  long offset,
                  long length)
    {
        this.filename = filename;
        this.offset = offset;
        this.length = length;

        // Will be set later
        this.buffer = null;
        this.bufferOffset = 0;
    }

    /**
     * @return the offset into its file of this Region.
     */
    public long getStart() {
        return this.offset;
    }

    /**
     * @return the offset into its file of the first byte after this Region.
     */
    public long getAfter() {
        return this.offset + this.length;
    }

    /**
     * @return the length of this region
     */
    public long getLength() {
        return this.length;
    }

    /**
     * @return the disk IO request fetching data for this Region.
     */
    public DiskReq getDiskReq() {
        return this.request;
    }

    /**
     * Set the buffer and bufferOffset of this Region.
     */
    public void setBuffer(MiniBuffer buffer, long bufferOffset) {
        this.buffer = buffer;
        this.bufferOffset = bufferOffset;
    }

    /**
     * Set the disk request for this Region.
     */
    public void setRequest(DiskReq request) {
        this.request = request;
    }

    /**
     * Compare two Regions.
     *
     * @throws IllegalArgumentException if they are not from the same file.
     */
    public int compareTo(Region r) {
        if (!this.filename.equals(r.filename)) {
            throw new IllegalArgumentException(
                    "Cannot compare regions of different files");
        }

        if (this.offset < r.offset) {
            return -1;
        } else if (this.offset == r.offset) {
            return 0;
        } else {
            return 1;
        }
    }

    /**
     * Split this region into two at the given offset (into the file).
     *
     * This object will retain the portion before the offset, while the
     * returned object will contain the remainder of the region.
     *
     * @param offset the offset into the file where this region is to be split.
     * @return a new Region with the portion of this Region after offset.
     * @throws IllegalArgumentException if offset is not in this region.
     */
    public Region splitAt(long offset) {
        // Sanity
        if (offset >= this.offset + this.length) {
            throw new IllegalArgumentException(
                    "The requested offset (" + offset
                    +") is beyond the end of this Region (" + this + ")");
        }

        // Create a new Region (the portion after `offset`)
        long newLengthPost = offset - this.offset; // length of part before split
        long newLengthPre = this.length - newLengthPost; // and after split
        Region r = new Region(
                this.filename,
                offset,
                newLengthPost,
                this.buffer,
                this.bufferOffset + newLengthPre);

        // Update this region
        this.length = newLengthPre;

        return r;
    }

    /**
     * Read the contents of this Region into the given buffer. This may block
     * waiting for IO.
     *
     * @param buffer the buffer to read this Region into.
     * @param bufferOffset the offset from the beginning of `buffer` where the
     * data should be read.
     * @throws IOException if there was an IOException during IO.
     */
    public void readToBuffer(byte[] buffer, long bufferOffset)
        throws IOException
    {
        this.buffer.readToBuffer(this.bufferOffset, buffer, bufferOffset);
    }

    public String toString() {
        return "Region { file=" + this.filename +
            ", offset=" + this.offset +
            ", length=" + this.length + " }";
    }
}
