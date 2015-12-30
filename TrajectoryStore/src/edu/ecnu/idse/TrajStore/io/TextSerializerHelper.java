package edu.ecnu.idse.TrajStore.io;

import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by zzg on 15-12-23.
 */
public final class TextSerializerHelper {

    final static byte[] digits = {
            '0' , '1' , '2' , '3' , '4' , '5' ,
            '6' , '7' , '8' , '9' , 'a' , 'b' ,
            'c' , 'd' , 'e' , 'f' , 'g' , 'h' ,
            'i' , 'j' , 'k' , 'l' , 'm' , 'n' ,
            'o' , 'p' , 'q' , 'r' , 's' , 't' ,
            'u' , 'v' , 'w' , 'x' , 'y' , 'z'
    };

    final static boolean[] HexadecimalChars;
    final static boolean[] DecimalChars;

    /**64 bytes to append to a string if necessary*/
    final static byte[] ToAppend = new byte[64];
    static {
        HexadecimalChars = new boolean[256];
        DecimalChars = new boolean[256];
        for (char i = 'a'; i <= 'f'; i++)
            HexadecimalChars[i] = true;
        for (char i = 'A'; i <= 'F'; i++)
            HexadecimalChars[i] = true;
        for (char i = '0'; i <= '9'; i++) {
            DecimalChars[i] = true;
            HexadecimalChars[i] = true;
        }
        HexadecimalChars['-'] = true;
        DecimalChars['-'] = true;

        Arrays.fill(ToAppend, (byte)' ');
    }

    public static void serializeDouble(double d, Text t, char toAppend) {
        byte[] bytes = Double.toString(d).getBytes();
        t.append(bytes, 0, bytes.length);
        if (toAppend != '\0') {
            t.append(new byte[] {(byte)toAppend}, 0, 1);
        }
    }

    public static double consumeDouble(Text text, char separator) {
        int i = 0;
        byte[] bytes = text.getBytes();
        // Skip until the separator or end of text
        while (i < text.getLength()
                && ((bytes[i] >= '0' && bytes[i] <= '9') || bytes[i] == 'e'
                || bytes[i] == 'E' || bytes[i] == '-' || bytes[i] == '+' || bytes[i] == '.'))
            i++;
        double d = Double.parseDouble(new String(bytes, 0, i));
        if (i < text.getLength() && bytes[i] == separator)
            i++;
        System.arraycopy(bytes, i, bytes, 0, text.getLength() - i);
        text.set(bytes, 0, text.getLength() - i);
        return d;
    }

    public static void serializeInt(int i, Text t, char toAppend) {
        // Calculate number of bytes needed to serialize the given long
        int bytes_needed = 0;
        int temp;
        if (i < 0) {
            bytes_needed++; // An additional
            temp = -i;
        } else {
            temp = i;
        }
        do {
            bytes_needed += 1;
            temp /= 10;
        } while (temp != 0);

        if (toAppend != '\0')
            bytes_needed++;

        // Reserve the bytes needed in the text
        t.append(ToAppend, 0, bytes_needed);
        // Extract the underlying buffer array and fill it directly
        byte[] buffer = t.getBytes();
        // Position of the next character to write in the text
        int position = t.getLength() - 1;
        if (toAppend != '\0')
            buffer[position--] = (byte) toAppend;

        // Negative sign is prepended separately for negative numbers
        boolean negative = false;
        if (i < 0) {
            i = -i;
            negative = true;
        }
        do {
            int digit = i % 10;
            buffer[position--] = digits[digit];
            i /= 10;
        } while (i != 0);
        if (negative)
            buffer[position--] = '-';
    }

    public static int deserializeInt(byte[] buf, int offset, int len) {
        boolean negative = false;
        if (buf[offset] == '-') {
            negative = true;
            offset++;
            len--;
        }
        int i = 0;
        while (len-- > 0) {
            i *= 10;
            i += buf[offset++] - '0';
        }
        return negative ? -i : i;
    }

    public static int consumeInt(Text text, char separator) {
        int i = 0;
        byte[] bytes = text.getBytes();
        // Skip until the separator or end of text
        while (i < text.getLength() && DecimalChars[bytes[i]])
            i++;
        int l = deserializeInt(bytes, 0, i);
        // If the first char after the long is the separator, skip it
        if (i < text.getLength() && bytes[i] == separator)
            i++;
        // Shift bytes after the long
        System.arraycopy(bytes, i, bytes, 0, text.getLength() - i);
        text.set(bytes, 0, text.getLength() - i);
        return l;
    }

    private static final byte[] Separators = {'[', '#', ',', ']'};
    private static final int MapStart = 0, KeyValueSeparator = 1,
            FieldSeparator = 2, MapEnd = 3;
    public static void consumeMap(Text text, Map<String, String> tags) {
        tags.clear();
        if (text.getLength() > 0) {
            byte[] tagsBytes = text.getBytes();
            if (tagsBytes[0] != Separators[MapStart])
                return;
            int i1 = 1;
            while (i1 < text.getLength() && tagsBytes[i1] != Separators[MapEnd]) {
                int i2 = i1 + 1;
                while (i2 < text.getLength() && tagsBytes[i2] != Separators[KeyValueSeparator])
                    i2++;
                String key = new String(tagsBytes, i1, i2 - i1);
                i1 = i2 + 1;

                i2 = i1 + 1;
                while (i2 < text.getLength() && tagsBytes[i2] != Separators[FieldSeparator] && tagsBytes[i2] != Separators[MapEnd])
                    i2++;
                String value = new String(tagsBytes, i1, i2 - i1);
                tags.put(key, value);
                i1 = i2;
                if (i1 < text.getLength() && tagsBytes[i1] == Separators[FieldSeparator])
                    i1++;
            }
            if (i1 < text.getLength())
                text.set(tagsBytes, i1, text.getLength() - i1);
        }
    }

    public static Text serializeMap(Text text, Map<String, String> tags) {
        if (!tags.isEmpty()) {
            boolean first = true;
            text.append(Separators, MapStart, 1);
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                if (first) {
                    first = false;
                } else {
                    first = true;
                    text.append(Separators, FieldSeparator, 1);
                }
                byte[] k = entry.getKey().getBytes();
                text.append(k, 0, k.length);
                text.append(Separators, KeyValueSeparator, 1);
                byte[] v = entry.getValue().getBytes();
                text.append(v, 0, v.length);
            }
            text.append(Separators, MapEnd, 1);
        }
        return text;
    }

    public static void serializeLong(long i, Text t, char toAppend) {
        // Calculate number of bytes needed to serialize the given long
        int bytes_needed = 0;
        long temp;
        if (i < 0) {
            bytes_needed++; // An additional
            temp = -i;
        } else {
            temp = i;
        }
        do {
            bytes_needed += 1;
            temp /= 10;
        } while (temp != 0);

        if (toAppend != '\0')
            bytes_needed++;

        // Reserve the bytes needed in the text
        t.append(ToAppend, 0, bytes_needed);
        // Extract the underlying buffer array and fill it directly
        byte[] buffer = t.getBytes();
        // Position of the next character to write in the text
        int position = t.getLength() - 1;

        if (toAppend != '\0')
            buffer[position--] = (byte) toAppend;

        // Negative sign is prepended separately for negative numbers
        boolean negative = false;
        if (i < 0) {
            i = -i;
            negative = true;
        }
        do {
            int digit = (int) (i % 10);
            buffer[position--] = digits[digit];
            i /= 10;
        } while (i != 0);
        if (negative)
            buffer[position--] = '-';
    }

    public static long consumeLong(Text text, char separator) {
        int i = 0;
        byte[] bytes = text.getBytes();
        // Skip until the separator or end of text
        while (i < text.getLength() && DecimalChars[bytes[i]])
            i++;
        long l = deserializeLong(bytes, 0, i);
        // If the first char after the long is the separator, skip it
        if (i < text.getLength() && bytes[i] == separator)
            i++;
        // Shift bytes after the long
        System.arraycopy(bytes, i, bytes, 0, text.getLength() - i);
        text.set(bytes, 0, text.getLength() - i);
        return l;
    }

    /**
     * Deserializes and consumes a long from the given text. Consuming means all
     * characters read for deserialization are removed from the given text.
     * If separator is non-zero, a long is read and consumed up to the first
     * occurrence of this separator. The separator is also consumed.
     * @param text
     * @param separator
     * @return
     */
    public static long consumeHexLong(Text text, char separator) {
        int i = 0;
        byte[] bytes = text.getBytes();
        // Skip until the separator or end of text
        while (i < text.getLength() && HexadecimalChars[bytes[i]])
            i++;
        long l = deserializeHexLong(bytes, 0, i);
        // If the first char after the long is the separator, skip it
        if (i < text.getLength() && bytes[i] == separator)
            i++;
        // Shift bytes after the long
        System.arraycopy(bytes, i, bytes, 0, text.getLength() - i);
        text.set(bytes, 0, text.getLength() - i);
        return l;
    }

    public static long deserializeHexLong(byte[] buf, int offset, int len) {
        boolean negative = false;
        if (buf[offset] == '-') {
            negative = true;
            offset++;
            len--;
        }
        long i = 0;
        while (len-- > 0) {
            i <<= 4;
            if (buf[offset] <= '9')
                i |= buf[offset++] - '0';
            else
                i |= buf[offset++] - 'a' + 10;
        }
        return negative ? -i : i;
    }

    public static long deserializeLong(byte[] buf, int offset, int len) {
        boolean negative = false;
        if (buf[offset] == '-') {
            negative = true;
            offset++;
            len--;
        }
        long i = 0;
        while (len-- > 0) {
            i *= 10;
            i += buf[offset++] - '0';
        }
        return negative ? -i : i;
    }

}

