
package org.apache.hadoop.mapred;

import static org.junit.Assert.*;

import java.io.File;
import java.io.PrintWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;

import java.util.Arrays;

import org.junit.Test;
import org.junit.BeforeClass;

import org.apache.hadoop.mapred.PrefetchedFile;

public class TestPrefetchedFile {
    public static final String TEST_FILE = "/tmp/pref_file_test.txt";
    public static final String TEST_CONTENTS =
        "YOYOYO\nLALALA\nTROLOLOLO\nDADADA\nTUN TUNUK TUN :P";

    @BeforeClass
    public static void setup() throws FileNotFoundException, IOException {
        // Open the test file
        File file = new File(TEST_FILE);

        // Delete the file in case it exists from a previous run
        file.delete();
        boolean created = file.createNewFile();
        assertTrue(created);

        FileOutputStream out;
        PrintWriter pw;

        // Write the test contents
        out = new FileOutputStream(file);
        pw = new PrintWriter(out);
        pw.write(TEST_CONTENTS);

        // Sync the file to make sure its contents will be seen hereafter
        pw.flush();
        out.getFD().sync();

        out.close();
    }

    @Test
    public void testOpenFile() throws FileNotFoundException, IOException{
        // Open a prefetched file (bypassing security option)
        File file = new File(TEST_FILE);
        PrefetchedFile pf = PrefetchedFile.forceSecureOpenForRandomRead(
                file,
                "r",
                "hehehe",
                "hohoho"
                );
    }

    @Test
    public void testReadByte() throws FileNotFoundException, IOException {
        // Open a prefetched file (bypassing security option)
        File file = new File(TEST_FILE);
        PrefetchedFile pf = PrefetchedFile.forceSecureOpenForRandomRead(
                file,
                "r",
                "hehehe",
                "hohoho"
                );

        // Read a byte from the file
        int x = pf.read();
        assertEquals(89, x); // 'Y'
    }

    @Test
    public void testReadOffset() throws FileNotFoundException, IOException {
        // Open a prefetched file (bypassing security option)
        File file = new File(TEST_FILE);
        PrefetchedFile pf = PrefetchedFile.forceSecureOpenForRandomRead(
                file,
                "r",
                "hehehe",
                "hohoho"
                );

        // Read from the file
        byte []buf = new byte[20];
        int bytes = pf.read(buf, 10, 10);
        assertTrue(10 >= bytes);

        String asStr = new String(Arrays.copyOfRange(buf, 10, 10 + bytes));

        assertEquals(TEST_CONTENTS.substring(0, bytes), asStr);
    }

    @Test
    public void testReadToBuf() throws FileNotFoundException, IOException {
        // Open a prefetched file (bypassing security option)
        File file = new File(TEST_FILE);
        PrefetchedFile pf = PrefetchedFile.forceSecureOpenForRandomRead(
                file,
                "r",
                "hehehe",
                "hohoho"
                );

        // Read from the file
        byte []buf = new byte[TEST_CONTENTS.length()];
        int bytes = pf.read(buf);
        assertTrue(TEST_CONTENTS.length() >= bytes);

        String asStr = new String(Arrays.copyOfRange(buf, 0, bytes));

        assertEquals(TEST_CONTENTS.substring(0, bytes), asStr);
    }
}
