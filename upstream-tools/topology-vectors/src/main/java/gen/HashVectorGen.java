package gen;

import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.ch.impl.HashFunctionPartitioner;

/**
 * Emits tests/vectors/murmur_hash3.hex by running the unmodified infinispan-core/commons 14.0.35.Final
 * classes. Line format: kind TAB input TAB outputs (space separated decimals).
 */
public class HashVectorGen {
   static String hex(byte[] b) {
      StringBuilder sb = new StringBuilder();
      for (byte x : b) sb.append(String.format("%02x", x));
      return sb.toString();
   }

   static String utf16(String s) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < s.length(); i++) sb.append(String.format("%04x", (int) s.charAt(i)));
      return sb.toString();
   }

   public static void main(String[] args) throws Exception {
      MurmurHash3 h = MurmurHash3.getInstance();
      HashFunctionPartitioner part = new HashFunctionPartitioner(256);
      HashFunctionPartitioner part7 = new HashFunctionPartitioner(7);
      try (PrintWriter out = new PrintWriter(args[0], "UTF-8")) {
         int[] seeds = {9001, 0, -5, 123456789};
         for (int len = 0; len <= 70; len++) {
            byte[] b = new byte[len];
            for (int i = 0; i < len; i++) b[i] = (byte) (i * 37 + len * 11 + 0x80);
            for (int seed : seeds) {
               long[] r = MurmurHash3.MurmurHash3_x64_128(b, seed);
               out.println("bytes128\t" + seed + ":" + hex(b) + "\t" + r[0] + " " + r[1]
                     + " " + MurmurHash3.MurmurHash3_x64_32(b, seed));
            }
            out.println("bytesHash\t" + hex(b) + "\t" + h.hash(b) + " " + part.getSegment(b) + " " + part7.getSegment(b));
         }
         long state = 0x1234_5678_9abc_def0L;
         for (int len = 1; len <= 9; len++) {
            long[] k = new long[len];
            for (int i = 0; i < len; i++) {
               state = state * 6364136223846793005L + 1442695040888963407L;
               k[i] = state;
            }
            for (int seed : seeds) {
               long[] r = MurmurHash3.MurmurHash3_x64_128(k, seed);
               StringBuilder in = new StringBuilder();
               for (long x : k) in.append(x).append(',');
               out.println("longs128\t" + seed + ":" + in + "\t" + r[0] + " " + r[1]
                     + " " + MurmurHash3.MurmurHash3_x64_32(k, seed));
            }
            StringBuilder in = new StringBuilder();
            for (long x : k) in.append(x).append(',');
            out.println("longsHash\t" + in + "\t" + MurmurHash3.hash(k));
         }
         String[] strings = {"", "a", "key", "hello world", "0123456789abcdef", "0123456789abcdefg",
               "a longer string that spans several sixteen byte blocks of utf-8 data",
               "café", "ééééééééé", "€ uro €",
               "😀 emoji 😀😀", "lone \ud83d high", "lone \ude00 low", "\ud83d", "\ude00x",
               "ÿþýüûúùø÷öõôóòñ",
               "编码测试字符串哈希", "id-1", "id-2", "id-3", "org.infinispan.CONFIG"};
         for (String s : strings) {
            out.println("string\t" + utf16(s) + "\t" + h.hash((Object) s) + " " + part.getSegment(s) + " " + part7.getSegment(s));
         }
         for (int i = 0; i < 300; i++) {
            String s = "key-" + i;
            out.println("string\t" + utf16(s) + "\t" + h.hash((Object) s) + " " + part.getSegment(s) + " " + part7.getSegment(s));
         }
         int[] ints = {0, 1, -1, 2, 255, 256, 65535, 65536, 0x7fffffff, 0x80000000, 0x01020304, 0xfffefdfc, 0x80808080, 123456789, -987654321};
         for (int i : ints) {
            out.println("int\t" + i + "\t" + h.hash(i) + " " + part.getSegment(Integer.valueOf(i)) + " " + part7.getSegment(Integer.valueOf(i)));
         }
         for (long l : new long[]{0L, 1L, -1L, 1L << 40, Long.MIN_VALUE, 123456789012345L}) {
            out.println("javaLong\t" + l + "\t" + h.hash((Object) Long.valueOf(l)) + " " + part.getSegment(Long.valueOf(l)));
         }
      }
   }
}
