
package org.apache.hadoop.mapred;

//import static org.apache.hadoop.mapred.Prefetcher.PREFETCHER;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.Prefetcher;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.annotations.VisibleForTesting;

public class PrefetchedFile extends RandomAccessFile {

    // The file we are reading
    private final File file;

    // The mode with which we are reading the file
    private final String mode;

    // The ID of the reducer that opened the file (used to inform the prefetcher).
    private final int reducerId;

    private static final Log LOG = LogFactory.getLog(PrefetchedFile.class);

    private Prefetcher prefetcher = new Prefetcher();

    /**
     * Construct a PrefetchedFile handler over the given file with the given mode.
     *
     * This behaves more or less like RandomAccessFile.
     */
    private PrefetchedFile(File f, String mode, int reducerId) throws IOException {
        super(f, mode);
        LOG.info("Creating a PrefetchedFile");
        this.file = f;
        this.mode = mode;
        this.reducerId = reducerId;
    }

    /**
     * Overrides the RandomAccessFile.read method to instead call the prefetcher.
     *
     * Reads a byte of data from this file. The byte is returned as an integer
     * in the range 0 to 255 (0x00-0x0ff). This method blocks if no input is
     * yet available.
     *
     * Although RandomAccessFile is not a subclass of InputStream, this method
     * behaves in exactly the same way as the InputStream.read() method of
     * InputStream.
     *
     * @return the next byte of data, or -1 if the end of the file has been reached.
     * @throws IOException if an I/O error occurs. Not thrown if end-of-file
     * has been reached.
     */
    @Override
    public int read() throws IOException {
        // Create a buffer of 1B to read into
        byte b[] = new byte[1];

        // Read the byte
        int bytes = read(b);
        assert(bytes == 1 || bytes == -1);

        // Return that byte as an integer or EOF
        if (bytes == 1) {
            byte []toInt = {b[0], 0,0,0};
            ByteBuffer bb = ByteBuffer.wrap(toInt);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            return bb.getInt();
        } else {
            return -1;
        }
    }

    /**
     * Overrides the RandomAccessFile.read method to instead call the prefetcher.
     *
     * Reads up to len bytes of data from this file into an array of bytes.
     * This method blocks until at least one byte of input is available.
     *
     * Although RandomAccessFile is not a subclass of InputStream, this method
     * behaves in exactly the same way as the InputStream.read(byte[], int,
     * int) method of InputStream.
     *
     * Parameters:
     * @param b the buffer into which the data is read.
     * @param off the start offset in array b at which the data is written.
     * @param len the maximum number of bytes read.
     * @return the total number of bytes read into the buffer, or -1 if there
     * is no more data because the end of the file has been reached.
     * @throws IOException If the first byte cannot be read for any reason
     * other than end of file, or if the random access file has been closed, or
     * if some other I/O error occurs.
     * @throws NullPointerException If b is null.
     * @throws IndexOutOfBoundsException If off is negative, len is negative,
     * or len is greater than b.length - off
     */
    @Override
    public int read(
            byte[] b,
            int off,
            int len
    ) throws IOException
    {
        // Create an array of the correct size to make sure we don't overread
        byte sub[] = new byte[len];

        // Read the file
        int bytes = read(sub);

        // Copy into the buffer given to us byte the caller at the given offset
        for (int i = 0; i < bytes; i++) {
            b[i + off] = sub[i];
        }

        // Return the number of bytes written
        return len;
    }

    /**
     * Overrides the RandomAccessFile.read method to instead call the prefetcher.
     *
     * Reads up to b.length bytes of data from this file into an array of
     * bytes. This method blocks until at least one byte of input is available.
     *
     * Although RandomAccessFile is not a subclass of InputStream, this method
     * behaves in exactly the same way as the InputStream.read(byte[]) method
     * of InputStream.
     *
     * @param b the buffer into which the data is read.
     * @return the total number of bytes read into the buffer, or -1 if there
     * is no more data because the end of this file has been reached.
     * @throws IOException If the first byte cannot be read for any reason
     * other than end of file, or if the random access file has been closed, or
     * if some other I/O error occurs.
     * @throws NullPointerException If b is null.
     */
    @Override
    public int read(byte[] b) throws IOException {
        // offset?
        LOG.info("Calling PrefetchedFile.read(b)");
        int bytes = prefetcher.read(
            this.file.getAbsolutePath(),
            getFilePointer(),
            b,
            this.reducerId
        );

        // Update the offset by seeking
        seek(getFilePointer() + bytes);

        // Return the number of bytes
        return bytes;
    }

    /**
     * Open the given File for random read access, verifying the expected user/
     * group constraints if security is enabled.
     *
     * Note that this function provides no additional security checks if hadoop
     * security is disabled, since doing the checks would be too expensive when
     * native libraries are not available.
     *
     * @param f file that we are trying to open
     * @param mode mode in which we want to open the random access file
     * @param expectedOwner the expected user owner for the file
     * @param expectedGroup the expected group owner for the file
     * @throws IOException if an IO error occurred or if the user/group does
     * not match when security is enabled.
     */
    public static PrefetchedFile openForRandomReadPrefetched(
            File f,
            String mode,
            String expectedOwner,
            String expectedGroup,
            int reducerId
    ) throws IOException
    {
        LOG.info("Calling PrefetchedFile.openForRandomReadPrefetched()");
        if (!UserGroupInformation.isSecurityEnabled()) {
            return new PrefetchedFile(f, mode, reducerId);
        }

        return forceSecureOpenForRandomRead(f,
                                            mode,
                                            expectedOwner,
                                            expectedGroup,
                                            reducerId);
    }

    /**
     * Same as openForRandomRead except that it will run even if security is off.
     * This is used by unit tests.
     */
    @VisibleForTesting
    protected static PrefetchedFile forceSecureOpenForRandomRead(
            File f,
            String mode,
            String expectedOwner,
            String expectedGroup,
            int reducerId
    ) throws IOException
    {
        PrefetchedFile raf = new PrefetchedFile(f, mode, reducerId);
        return raf;
        /*
         * TODO: eventually we should fix this...
        boolean success = false;
        try {
            PosixFileAttributes attrs =
                Files.getFileAttributeView(f, PosixFileAttributeView.class)
                .readAttributes();
            checkStat(f, attrs.owner().getName(), attrs.group().getName(), expectedOwner,
                    expectedGroup);
            success = true;
            return raf;
        } finally {
            if (!success) {
                raf.close();
            }
        }
        */
    }

    private static void checkStat(File f, String owner, String group,
            String expectedOwner,
            String expectedGroup) throws IOException {
        boolean success = true;
        if (expectedOwner != null &&
                !expectedOwner.equals(owner)) {
            if (Path.WINDOWS) {
                UserGroupInformation ugi =
                    UserGroupInformation.createRemoteUser(expectedOwner);
                final String adminsGroupString = "Administrators";
                success = owner.equals(adminsGroupString)
                    && ugi.getGroups().contains(adminsGroupString);
            } else {
                success = false;
            }
        }
        if (!success) {
            throw new IOException(
                    "Owner '" + owner + "' for path " + f + " did not match " +
                    "expected owner '" + expectedOwner + "'");
        }
    }
}
