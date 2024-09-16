package org.urbcomp.startdb.selfstar.decompressor.xor;

import org.urbcomp.startdb.selfstar.utils.InputBitStream;
import org.urbcomp.startdb.selfstar.utils.PostOfficeSolver;

import java.io.IOException;
import java.util.Arrays;

public class ChimpNAdaXORDecompressor implements IXORDecompressor {

    private final static long END_SIGN = Double.doubleToLongBits(Double.NaN);
    private final long[] storedValues;
    private final int previousValues;
    private final int previousValuesLog2;
    private final int initialFill;
    private int[] leadingRepresentation = {0, 8, 12, 16, 18, 20, 22, 24};
    private int storedLeadingZeros = Integer.MAX_VALUE;
    private long storedVal = 0;
    private int current = 0;
    private boolean first = true;
    private boolean endOfStream = false;
    private InputBitStream in;
    private int leadingBitsPerValue;

    public ChimpNAdaXORDecompressor(int previousValues) {
        this(new byte[0], previousValues);
    }

    public ChimpNAdaXORDecompressor(byte[] bs, int previousValues) {
        in = new InputBitStream(bs);
        this.previousValues = previousValues;
        this.previousValuesLog2 = (int) (Math.log(previousValues) / Math.log(2));
        this.initialFill = previousValuesLog2 + 6;
        this.storedValues = new long[previousValues];
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
        current = 0;
        first = true;
        endOfStream = false;
        Arrays.fill(storedValues, 0);
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
            storedValues[current] = storedVal;
            if (storedValues[current] == END_SIGN) {
                endOfStream = true;
            }
        } else {
            nextValue();
        }
    }

    private void nextValue() throws IOException {
        // Read value
        int flag = in.readInt(2);
        long value;
        switch (flag) {
            case 3:
                storedLeadingZeros = leadingRepresentation[in.readInt(leadingBitsPerValue)];
                value = in.readLong(64 - storedLeadingZeros);
                value = storedVal ^ value;

                if (value == END_SIGN) {
                    endOfStream = true;
                    return;
                } else {
                    storedVal = value;
                    current = (current + 1) % previousValues;
                    storedValues[current] = storedVal;
                }
                break;
            case 2:
                value = in.readLong(64 - storedLeadingZeros);
                value = storedVal ^ value;
                if (value == END_SIGN) {
                    endOfStream = true;
                    return;
                } else {
                    storedVal = value;
                    current = (current + 1) % previousValues;
                    storedValues[current] = storedVal;
                }
                break;
            case 1:
                int fill = this.initialFill + leadingBitsPerValue;
                int temp = in.readInt(fill);
                int index = temp >>> (fill -= previousValuesLog2) & (1 << previousValuesLog2) - 1;
                storedLeadingZeros = leadingRepresentation[temp >>> (fill -= leadingBitsPerValue) & (1 << leadingBitsPerValue) - 1];
                int significantBits = temp >>> (fill - 6) & (1 << 6) - 1;
                storedVal = storedValues[index];
                if (significantBits == 0) {
                    significantBits = 64;
                }
                int storedTrailingZeros = 64 - significantBits - storedLeadingZeros;
                value = in.readLong(64 - storedLeadingZeros - storedTrailingZeros);
                value <<= storedTrailingZeros;
                value = storedVal ^ value;
                if (value == END_SIGN) {
                    endOfStream = true;
                    return;
                } else {
                    storedVal = value;
                    current = (current + 1) % previousValues;
                    storedValues[current] = storedVal;
                }
                break;
            default:
                // else -> same value as before
                storedVal = storedValues[(int) in.readLong(previousValuesLog2)];
                current = (current + 1) % previousValues;
                storedValues[current] = storedVal;
                break;
        }
    }

}
