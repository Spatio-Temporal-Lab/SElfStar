package org.urbcomp.startdb.selfstar.decompressor32.xor;

import org.urbcomp.startdb.selfstar.utils.Elf32Utils;
import org.urbcomp.startdb.selfstar.utils.InputBitStream;

import java.io.IOException;

/**
 * Decompresses a compressed stream created by the Compressor. Returns pairs of timestamp and floating point value.
 */
public class ChimpXORDecompressor32 implements IXORDecompressor32 {

    public final static short[] leadingRepresentation = {0, 8, 12, 16, 18, 20, 22, 24};
    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedVal = 0;
    private boolean first = true;
    private boolean endOfStream = false;
    private InputBitStream in;

    public ChimpXORDecompressor32() {
        this(new byte[0]);
    }

    public ChimpXORDecompressor32(byte[] bs) {
        in = new InputBitStream(bs);
    }

    public InputBitStream getInputStream() {
        return in;
    }

    @Override
    public void setBytes(byte[] bs) {
        in = new InputBitStream(bs);
    }

    @Override
    public void refresh() {
        first = true;
        endOfStream = false;
    }

    /**
     * Returns the next pair in the time series, if available.
     *
     * @return Pair if there's next value, null if series is done.
     */
    public Float readValue() {
        try {
            next();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
        if (endOfStream) {
            return null;
        }
        return Float.intBitsToFloat(storedVal);
    }

    private void next() throws IOException {
        if (first) {
            first = false;
            storedVal = in.readInt(32);
            endOfStream = storedVal == Elf32Utils.END_SIGN;
        } else {
            nextValue();
        }
    }

    private void nextValue() throws IOException {

        int significantBits;
        int value;
        // Read value
        int flag = in.readInt(2);
        switch (flag) {
            case 3:
                // New leading zeros
                storedLeadingZeros = leadingRepresentation[in.readInt(3)];
                value = in.readInt(32 - storedLeadingZeros);
                value = storedVal ^ value;

                endOfStream = value == Elf32Utils.END_SIGN;
                storedVal = value;
                break;
            case 2:
                value = in.readInt(32 - storedLeadingZeros);
                value = storedVal ^ value;

                endOfStream = value == Elf32Utils.END_SIGN;
                storedVal = value;
                break;
            case 1:
                storedLeadingZeros = leadingRepresentation[in.readInt(3)];
                significantBits = in.readInt(5);
                if (significantBits == 0) {
                    significantBits = 32;
                }
                int storedTrailingZeros = 32 - significantBits - storedLeadingZeros;
                value = in.readInt(32 - storedLeadingZeros - storedTrailingZeros);
                value <<= storedTrailingZeros;
                value = storedVal ^ value;

                endOfStream = value == Elf32Utils.END_SIGN;
                storedVal = value;
                break;
            default:
        }
    }

}
