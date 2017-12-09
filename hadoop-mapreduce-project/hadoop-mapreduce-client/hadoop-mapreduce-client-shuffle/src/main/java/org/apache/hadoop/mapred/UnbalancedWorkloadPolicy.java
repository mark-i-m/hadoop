
package org.apache.hadoop.mapred;

import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.HashMap;

import org.apache.hadoop.mapred.PrefetchPolicy;
import org.apache.hadoop.mapred.PrefetchPolicy.Prefetch;

/**
 * A policy that makes no predictions but just forwards all requests to the
 * prefetcher.
 */
public class UnbalancedWorkloadPolicy implements PrefetchPolicy {

    private class Reducer implements Comparable<Reducer> {
        public int id;
        public long totalBytesRequested;
        public LinkedList<Prefetch> requests = new LinkedList<>();

        @Override
        public int compareTo(Reducer r) {
            return -(int)(this.totalBytesRequested - r.totalBytesRequested);
        }
    }

    private HashMap<Integer, Reducer> emptyReducers = new HashMap<>();
    private HashMap<Integer, Reducer> nonEmptyReducers = new HashMap<>();
    private PriorityQueue<Reducer> queuedReducers = new PriorityQueue<>();

    public PrefetchPolicy.Prefetch next(String filename,
                                        long offset,
                                        long length,
                                        long memUsage,
                                        long memBound,
                                        int reducerId)
    {
        Prefetch prefetch = new Prefetch(filename, offset, length);

        // If the element is in the hashset, then remove it, add the the new
        // prefetch, and insert to the priority queue.
        if (this.emptyReducers.containsKey(reducerId)) {
            Reducer r = this.emptyReducers.remove(reducerId);
            r.totalBytesRequested += length;
            r.requests.addLast(prefetch);
            this.queuedReducers.add(r);
            this.nonEmptyReducers.put(r.id, r);
        }

        // Otherwise, search the priority queue to see if there are other
        // requests from the reducer. Update the number of requested bytes and
        // the requests, and then reinsert to the queue.
        else if (this.nonEmptyReducers.containsKey(reducerId)) {
            Reducer r = this.nonEmptyReducers.remove(reducerId);
            this.queuedReducers.remove(r);
            r.totalBytesRequested += length;
            r.requests.addLast(prefetch);
            this.queuedReducers.add(r);
            this.nonEmptyReducers.put(r.id, r);
        }

        // Otherwise, create a new Reducer object
        else {
            Reducer r = new Reducer();
            r.id = reducerId;
            r.totalBytesRequested = length;
            r.requests.addLast(prefetch);
            this.queuedReducers.add(r);
            this.nonEmptyReducers.put(r.id, r);
        }

        // Get the non-empty reducer with the most requested bytes so far.
        while (true) {
            Reducer most = this.queuedReducers.poll();
            if (most == null) {
                return null;
            }
            this.nonEmptyReducers.remove(most.id);

            Prefetch next = null;

            // If that reducer has any requests, then pop the oldest one.
            if (most.requests.size() > 0) {
                next = most.requests.removeFirst();

                if (next != null) {
                    if (next.length <= (memBound - memUsage)) {
                        // If the reducer is not yet empty, put it back in the non-empty queue
                        if (most.requests.size() > 0) {
                            this.queuedReducers.add(most);
                            this.nonEmptyReducers.put(most.id, most);
                        }

                        // Otherwise, put it in the empty list
                        else {
                            this.emptyReducers.put(most.id, most);
                        }

                        return next;
                    } else {
                        // Not enough memory? Put the request back at the head of the queue.
                        most.requests.addFirst(next);

                        this.queuedReducers.add(most);
                        this.nonEmptyReducers.put(most.id, most);

                        return null;
                    }
                }
            }
        }
    }
}

