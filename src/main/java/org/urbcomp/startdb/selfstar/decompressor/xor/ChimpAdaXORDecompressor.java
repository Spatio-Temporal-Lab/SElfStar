package org.urbcomp.startdb.selfstar.decompressor.xor;

import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.InputBitStream;
import org.urbcomp.startdb.selfstar.utils.PostOfficeSolver;

import java.io.IOException;
import java.util.Arrays;

/**
 * Decompresses a compressed stream created by the Compressor. Returns pairs of timestamp and floating point value.
 */
public class ChimpAdaXORDecompressor implements IXORDecompressor {

    private int[] leadingRepresentation = {0, 8, 12, 16, 18, 20, 22, 24};

    private int leadingBitsPerValue;

    private int storedLeadingZeros = Integer.MAX_VALUE;
    private long storedVal = 0;
    private boolean first = true;
    private boolean endOfStream = false;
    private InputBitStream in;

    public ChimpAdaXORDecompressor() {
        this(new byte[0]);
    }

    public ChimpAdaXORDecompressor(byte[] bs) {
        in = new InputBitStream(bs);
    }

    private void initLeadingRepresentation() {
        try {
            int num = in.readInt(5);
            if (num == 0) {
                num = 32;
            }
            leadingBitsPerValue = PostOfficeSolver.positionLength2Bits[num];
            leadingRepresentation = new int[num];
            for (int i = 0; i < num; i++) {
                leadingRepresentation[i] = in.readInt(6);
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
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
        storedLeadingZeros = Integer.MAX_VALUE;
        storedVal = 0;
        first = true;
        endOfStream = false;
        Arrays.fill(leadingRepresentation, 0);
        leadingBitsPerValue = 0;
    }

    /**
     * Returns the next pair in the time series, if available.
     *
     * @return Pair if there's next value, null if series is done.
     */
    public Double readValue() {
        try {
            next();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
        if (endOfStream) {
            return null;
        }
        return Double.longBitsToDouble(storedVal);
    }

    private void next() throws IOException {
        if (first) {
            initLeadingRepresentation();
            first = false;
            storedVal = in.readLong(64);
            if (storedVal == Elf64Utils.END_SIGN) {
                endOfStream = true;
            }
        } else {
            nextValue();
        }
    }

    private void nextValue() throws IOException {

        int significantBits;
        long value;
        // Read value
        int flag = in.readInt(2);
        switch (flag) {
            case 3:
                // New leading zeros
                storedLeadingZeros = leadingRepresentation[in.readInt(leadingBitsPerValue)];
                value = in.readLong(64 - storedLeadingZeros);
                value = storedVal ^ value;
                if (value == Elf64Utils.END_SIGN) {
                    endOfStream = true;
                    return;
                } else {
                    storedVal = value;
                }
                break;
            case 2:
                value = in.readLong(64 - storedLeadingZeros);
                value = storedVal ^ value;
                if (value == Elf64Utils.END_SIGN) {
                    endOfStream = true;
                    return;
                } else {
                    storedVal = value;
                }
                break;
            case 1:
                storedLeadingZeros = leadingRepresentation[in.readInt(leadingBitsPerValue)];
                significantBits = in.readInt(6);
                if (significantBits == 0) {
                    significantBits = 64;
                }
                int storedTrailingZeros = 64 - significantBits - storedLeadingZeros;
                value = in.readLong(64 - storedLeadingZeros - storedTrailingZeros);
                value <<= storedTrailingZeros;
                value = storedVal ^ value;
                if (value == Elf64Utils.END_SIGN) {
                    endOfStream = true;
                    return;
                } else {
                    storedVal = value;
                }
                break;
            default:
        }
    }

}
