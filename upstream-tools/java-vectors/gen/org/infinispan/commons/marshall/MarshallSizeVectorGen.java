package org.infinispan.commons.marshall;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;

/**
 * Real upstream vectors for MarshallUtil.marshallSize (6-bit-first varint), compiled and run
 * directly against the unmodified upstream commons/all MarshallUtil.java.
 */
public class MarshallSizeVectorGen {
    public static void main(String[] args) throws Exception {
        PrintStream out = new PrintStream(new FileOutputStream(args[0]));
        int[] values = {
            0, 1, 62, 63, 64, 65, 100, 127, 128, 4095, 4096, 8191, 8192,
            65535, 65536, 1000000, Integer.MAX_VALUE, -1, -2, Integer.MIN_VALUE
        };
        for (int v : values) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            MarshallUtil.marshallSize(dos, v);
            dos.flush();
            byte[] bytes = baos.toByteArray();
            StringBuilder sb = new StringBuilder();
            for (byte b : bytes) sb.append(String.format("%02x", b));
            out.println(v + "\t" + sb);
        }
        out.close();
    }
}
