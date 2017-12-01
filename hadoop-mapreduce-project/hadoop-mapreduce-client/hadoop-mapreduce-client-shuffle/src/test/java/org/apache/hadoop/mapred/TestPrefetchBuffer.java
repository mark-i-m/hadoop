
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

import org.apache.hadoop.mapred.PrefetchBuffer;

public class TestPrefetchBuffer {
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
    public void testPrefRead() throws FileNotFoundException, IOException {
        // Create a new PrefetchBuffer for this test
        PrefetchBuffer pb = new PrefetchBuffer();

        // Submit a prefetch
        pb.prefetch(TEST_FILE, 2, TEST_CONTENTS.length() - 2);

        // Read the file
        byte [] buf = new byte[TEST_CONTENTS.length() - 4];
        pb.read(TEST_FILE, 2, buf);

        // Correctness check
        String asStr = new String(Arrays.copyOfRange(buf, 0, buf.length));
        
        assertEquals(TEST_CONTENTS.substring(2, 2 + buf.length), asStr);
    }
}
