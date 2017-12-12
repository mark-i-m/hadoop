
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
    private HashMap<String, TreeSet<Region>> regions = new HashMap<>();
    private LinkedList<DiskReq> reqQueue = new LinkedList<>();

    // Keep track of how much memory is being used.
    private Long memoryUsage = (long)0;

    // A thread that just fetches stuff from disk. It sits around waiting
    // for requests in reqQueue and fullfilling such requests.
    private final PrefetchThread prefThread = new PrefetchThread();

    /**
     * Log object for logging...
     */
    private static final Log LOG = LogFactory.getLog(PrefetchBuffer.class);


    ///////////////////////////////////////////////////////////////////////////
    // Constructor
    ///////////////////////////////////////////////////////////////////////////

    public PrefetchBuffer() {
        this.prefThread.setDaemon(true);
        this.prefThread.setName("PrefetchThread");
        this.prefThread.start();
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
    public void prefetch(
            String filename,
            long offset,
            long length)
        throws FileNotFoundException, IOException
    {
        LOG.info("PrefetchBuffer.Prefetch " + filename
                + " off=" + offset + " len=" + length);

        // Lock the whole buffer briefly
        TreeSet<Region> fRegions;
        synchronized (this) {
            // Get the regions for the file
            fRegions = this.regions.get(filename);
            if (fRegions == null) {
                fRegions = new TreeSet<>();
                this.regions.put(filename, fRegions);
            }
        }

        synchronized (fRegions) {
            // Find unrequested parts of this request
            ArrayList<Region> gaps = findGaps(filename, offset, length);

            // Request all gaps
            insertAndEnqueueAll(filename, gaps);

            // Get a sorted set of all regions covered by the given range of the file.
            // This set will possibly break up any regions on the ends, so we can just
            // increment the reference counts without fear!
            ArrayList<Region> covered = getRange(filename, offset, length);

            for (Region r : covered) {
                r.incrementCount();
            }
        }
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
        LOG.info("PrefetchBuffer.Read " + filename
                + " off=" + offset + " len=" + buf.length);

        // Lock the whole buffer briefly
        TreeSet<Region> fRegions;
        synchronized (this) {
            // Get the regions for the file
            fRegions = this.regions.get(filename);
            if (fRegions == null) {
                fRegions = new TreeSet<>();
                this.regions.put(filename, fRegions);
            }
        }

        // Now only lock that file's regions
        long length;
        synchronized (fRegions) {
            // Compute the number of bytes to read. This is the minimum of the number
            // of prefetched bytes and the length of the file.
            length = Math.min(fRegions.last().getAfter(), buf.length);

            // Get a sorted set of all regions covered by the given range of the file.
            // This set will possibly break up any regions on the ends, so we can just
            // decrement the reference counts without fear!
            ArrayList<Region> covered = getRange(filename, offset, length);

            // Read each region to the buffer. As we do so, decrement it's reference
            // count, and if the reference count reaches 0, deallocate.
            long soFar = 0;
            for (Region r : covered) {
                r.readToBuffer(buf, soFar);
                soFar += r.getLength();
                r.decrementCount();
                if (!r.isReferenced()) {
                    fRegions.remove(r);
                }
            }
        }

        // Keep a rough estimate of how much space is being used. Note: this is
        // possibly _underestimating_ how much space is used, since there may
        // be an unread region holding on to the first and last buffers of the
        // range.
        synchronized (this.memoryUsage) {
            this.memoryUsage -= length;
        }

        return (int) length;
    }

    /**
     * The amount of memory usage of this buffer.
     */
    public long memUsage() {
        //LOG.info("PrefetchBuffer.memUsage: synchronized being executed");
        synchronized (this.memoryUsage) {
            return this.memoryUsage;
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Helpers and buffer management
    ///////////////////////////////////////////////////////////////////////////

    /**
     * This method is basically a wrapper around TreeSet.subSet that accepts
     * `long` instead of `Region` and it adds in any region that overlaps the
     * beginning of the given range.
     */
    private static SortedSet<Region> getTreeSubSet(TreeSet<Region> tree,
                                                    String filename,
                                                    long start,
                                                    long end)
    {
        // Create a couple of dummy regions so we can call subSet
        Region dummyStart = new Region(filename, start, 0, null, 0);
        Region dummyEnd = new Region(filename, end, 0, null, 0);

        SortedSet<Region> set = tree.subSet(dummyStart, dummyEnd);
        TreeSet<Region> newSet = new TreeSet<>();
        newSet.addAll(set);

        // Also toss in any first Regions
        Region first = tree.floor(dummyStart);
        if (first != null) {
            newSet.add(first);
        }

        return newSet;
    }

    /**
     * @return if some thread has previously called `prefetch` for the given
     * range of the given file.
     */
    public boolean wasRequested(String filename, long offset, long length) {
        long soFar = offset;

        while (soFar < offset + length) {
            long amt = wasOffsetRequested(filename, soFar);

            if (amt < 0) {
                LOG.info("Not Requested: " + filename + ", off=" + offset +
                        ", len=" + offset + "; Only found up to off=" + soFar);
                return false;
            }

            soFar += amt;
        }

        return true;
    }

    /**
     * @return -1 if the given offset of the given file was never requested, or
     * some positive number of bytes requested otherwise.
     */
    private long wasOffsetRequested(String filename, long offset) {
        // Get prefetches for this file
        TreeSet<Region> fRegions;
        synchronized (this) {
            fRegions = this.regions.get(filename);
        }

        // No prefetches?
        if (fRegions == null) {
            return -1;
        }

        // Look for the greatest prefetch that starts before at the given offset
        Region region;
        synchronized (fRegions) {
            region = fRegions.floor(new Region(filename, offset, 0));
        }

        // No prefetches?
        if (region == null) {
            return -1;
        }

        // Check if that prefetch covers any of the given range
        if (region.getStart() + region.getLength() <= offset) {
            return -1;
        }

        return region.getStart() + region.getLength() - offset;
    }

    /**
     * Finds all previously unrequested Regions in the given range of the file,
     * creates new Regions for them, _allocates_ memory to back IO for them, and
     * returns the new Regions _without inserting or enqueuing them_.
     *
     * NOTE: this call should be made while holding the file's region lock, but
     * not the monitor lock!
     */
    private ArrayList<Region> findGaps(String filename, long offset, long length) {
        // Get the regions for the file
        TreeSet<Region> fRegions;
        synchronized (this) {
            fRegions  = this.regions.get(filename);
            if (fRegions == null) {
                fRegions = new TreeSet<>();
                this.regions.put(filename, fRegions);
            }
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
        long latestHandled = offset;
        ArrayList<Region> newRegions = new ArrayList<>(); // used below

        for (Region r : covered) {
            long regionStart = r.getStart();

            // If there is space between the beginning of `r` and
            // `latestHandled`, we need to create a new Region. However, since
            // a buffer can only contain so much data, we need to make sure we
            // respect the maximum size of any buffer and break up large
            // Regions appropriately.
            if (regionStart > latestHandled) {
                long spaceNeeded = regionStart - latestHandled;
                long nextOffset = latestHandled;

                while (spaceNeeded > 0) {
                    long newRLen = Math.min(spaceNeeded, MiniBuffer.BUFFER_SIZE);

                    newRegions.add(new Region(filename, nextOffset, newRLen));

                    spaceNeeded -= newRLen;
                    nextOffset += newRLen;
                }

                latestHandled = r.getAfter();
            }
        }

        // If there is still some space at the end of the requested range, create
        // Regions for it now.
        long spaceNeeded = length - (latestHandled - offset);

        while (spaceNeeded > 0) {
            long newRLen = Math.min(spaceNeeded, MiniBuffer.BUFFER_SIZE);

            newRegions.add(new Region(filename, latestHandled, newRLen));

            spaceNeeded -= newRLen;
            latestHandled += newRLen;
        }

        // allocate memory, setup disk IO requests, update Regions
        long mem = MiniBuffer.allocateRegions(filename, newRegions);
        synchronized (this.memoryUsage) {
            this.memoryUsage += mem;
        }

        // Done!
        return newRegions;
    }

    /**
     * Sorts, inserts, and enqueues the given list of regions.
     *
     * The sorting is done to minimize seeking.
     *
     * NOTE: this call should be made while holding the file's region lock, but
     * not the monitor lock or the request queue lock!
     */
    private void insertAndEnqueueAll(String filename, ArrayList<Region> list) {
        // Sort the regions
        Collections.sort(list);

        if (list.size() == 0) {
            return;
        }

        // Insert into the tree.
        TreeSet<Region> fRegions;
        synchronized (this) {
            fRegions = this.regions.get(filename);
            if (fRegions == null) {
                fRegions = new TreeSet<>();
                this.regions.put(filename, fRegions);
            }
        }

        fRegions.addAll(list);

        // Collect the set of disk requests for the given list of regions and
        // enqueue them all.
        ArrayList<DiskReq> requests = new ArrayList<>();
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
     * NOTE: this call should be made while holding the file's region lock, but
     * not the monitor lock or the request queue lock!
     *
     * @throws IllegalStateException if the range contains portions of the file
     * which have never been previously requested.
     */
    private ArrayList<Region> getRange(String filename, long offset, long length) {
        // Get the regions for the file
        TreeSet<Region> fRegions;
        synchronized (this) {
            fRegions = this.regions.get(filename);
            if (fRegions == null) {
                fRegions = new TreeSet<>();
                this.regions.put(filename, fRegions);
            }
        }

        // Get a sorted set of all regions covered by the given range of the file
        SortedSet<Region> covered =
            getTreeSubSet(fRegions, filename, offset, offset + length);

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

        if (soFar < offset + length) {
            throw new IllegalStateException(
                    "Requested range (offset=" + offset +
                    ", length=" + length +
                    ") from file " + filename +
                    ", but offset " + soFar +
                    " was never prefetched!"
                    );
        }

        // If needed break the first and last Regions so that we have a set of
        // regions that perfectly the requested range.
        Region first = covered.first();
        if (first.getStart() != offset) {
            Region secondHalf = first.splitAt(offset);
            fRegions.add(secondHalf);
            covered.remove(first);
            covered.add(secondHalf);
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
                DiskReq next = null;

                // Pop the queue head while locking the queue
                //LOG.info("PrefetchThread.run: synchronized being executed");
                synchronized (reqQueue) {
                    if (reqQueue.size() > 0) {
                        next = reqQueue.removeFirst();
                    }
                }
                //LOG.info("PrefetchThread.run: synchronized finished");

                // If no requests, sleep until later
                if (next == null) {
                    try {
                        Thread.sleep(100); // 100ms
                    } catch (InterruptedException ie) {
                        // It is OK!
                    }
                    continue;
                }

                // LOG.info(next);

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
                    if (next != null) {
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

    private static final Log LOG = LogFactory.getLog(MiniBuffer.class);

    /**
     * Wait for the data in this buffer to become available.
     */
    private synchronized void waitForData() {
        //LOG.info("MiniBuffer.waitForData: synchronized method");
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
        //LOG.info("MiniBuffer.dataReady: synchronized method");
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
     * @return the amount of memory allocated (in bytes).
     *
     * @throws IllegalArgumentException if any of the Regions is too
     * large (size greater than BUFFER_SIZE).
     */
    public static long allocateRegions(String filename, ArrayList<Region> regions) {
        // No regions... no problems
        if (regions.size() == 0) {
            return 0;
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
        long memoryUsage = 0;

        for (Region r : regions) {
            // Does the region fit into the latest buffer?
            if (latestBuf.size() == 0 ||
                latestBufLen + r.getLength() <= MiniBuffer.BUFFER_SIZE)
            {
                latestBuf.add(r);
            } else {
                // End the latestBuf and start a new one
                MiniBuffer buffer = new MiniBuffer();
                memoryUsage += MiniBuffer.BUFFER_SIZE;

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

        // Finish up the last regions
        MiniBuffer buffer = new MiniBuffer();
        memoryUsage += MiniBuffer.BUFFER_SIZE;

        long bufferOffset = 0;
        for (Region bufr : latestBuf) {
            bufr.setBuffer(buffer, bufferOffset);
            bufferOffset += bufr.getLength();
        }

        reqBuffers.add(buffer);

        long start = latestReq.get(0).getStart();
        long end = latestReq.get(latestReq.size()-1).getAfter();
        long length = end - start;

        DiskReq request = new DiskReq(filename, start, length);

        for (Region reqr : latestReq) {
            reqr.setRequest(request);
        }

        request.addBuffers(reqBuffers);

        return memoryUsage;
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

    @Override
    public String toString() {
        return "DiskReq { file=" + this.filename +
            ", offset=" + this.offset +
            ", length=" + this.length + " }";
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

    // The atomic reference count of this Region, denoting how many consumers
    // it has.
    //
    // NOTE: the reference count has the following invariant: Any thread that
    // increments the reference count should depend on _the whole Region_. If
    // a thread doesn't depend on the whole region, it should split the region
    // and increment the portion it needs.
    private Long refCount;

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
        this.refCount = 0l;
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
        this.refCount = 0l;
    }

    /**
     * Increment refCount atomically
     */
    public void incrementCount() {
        synchronized (this.refCount) {
            this.refCount += 1;
        }
    }

    /**
     * Decrement refCount atomically
     */
    public void decrementCount() {
        synchronized (this.refCount) {
            this.refCount -= 1;
        }
    }

    /**
     * @return true iff refCount > 0
     */
    public boolean isReferenced() {
        synchronized (this.refCount) {
            return this.refCount > 0;
        }
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
    @Override
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
     * Both Regions will have the same reference count as before. The caller
     * needs to increment the appropriate reference count.
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

        r.refCount = this.refCount;

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

    @Override
    public String toString() {
        return "Region { file=" + this.filename +
            ", offset=" + this.offset +
            ", length=" + this.length + " }";
    }
}
