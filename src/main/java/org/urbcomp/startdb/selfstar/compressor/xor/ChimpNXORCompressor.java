package org.urbcomp.startdb.selfstar.compressor.xor;

import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.util.Arrays;

/**
 * Implements the Chimp128 time series compression. Value compression
 * is for floating points only.
 *
 * @author Panagiotis Liakos
 */
public class ChimpNXORCompressor implements IXORCompressor {

    private final static short[] leadingRepresentation = {0, 0, 0, 0, 0, 0, 0, 0,
            1, 1, 1, 1, 2, 2, 2, 2,
            3, 3, 4, 4, 5, 5, 6, 6,
            7, 7, 7, 7, 7, 7, 7, 7,
            7, 7, 7, 7, 7, 7, 7, 7,
            7, 7, 7, 7, 7, 7, 7, 7,
            7, 7, 7, 7, 7, 7, 7, 7,
            7, 7, 7, 7, 7, 7, 7, 7
    };
    private final static short[] leadingRound = {0, 0, 0, 0, 0, 0, 0, 0,
            8, 8, 8, 8, 12, 12, 12, 12,
            16, 16, 18, 18, 20, 20, 22, 22,
            24, 24, 24, 24, 24, 24, 24, 24,
            24, 24, 24, 24, 24, 24, 24, 24,
            24, 24, 24, 24, 24, 24, 24, 24,
            24, 24, 24, 24, 24, 24, 24, 24,
            24, 24, 24, 24, 24, 24, 24, 24
    };
    private final long[] storedValues;
    private final int threshold;
    private final int previousValues;
    private final int setLsb;
    private final int[] indices;
    private final int flagOneSize;
    private final int flagZeroSize;
    private final int capacity;
    private int storedLeadingZeros = Integer.MAX_VALUE;
    private boolean first = true;
    private OutputBitStream out;
    private int index = 0;
    private int current = 0;

    // We should have access to the series?
    public ChimpNXORCompressor(int previousValues) {
        this(previousValues, 1000);
    }

    public ChimpNXORCompressor(int previousValues, int block) {
        capacity = block;
        out = new OutputBitStream(
                new byte[(int) (((capacity + 1) * 8 + capacity / 8 + 1) * 1.2)]);
        this.previousValues = previousValues;
        int previousValuesLog2 = (int) (Math.log(previousValues) / Math.log(2));
        this.threshold = 6 + previousValuesLog2;
        this.setLsb = (int) Math.pow(2, threshold + 1) - 1;
        this.indices = new int[(int) Math.pow(2, threshold + 1)];
        this.storedValues = new long[previousValues];
        this.flagZeroSize = previousValuesLog2 + 2;
        this.flagOneSize = previousValuesLog2 + 11;
    }

    public OutputBitStream getOutputStream() {
        return out;
    }

    @Override
    public byte[] getOut() {
        return out.getBuffer();
    }

    @Override
    public void refresh() {
        out = new OutputBitStream(
                new byte[(int) (((capacity + 1) * 8 + capacity / 8 + 1) * 1.2)]);
        storedLeadingZeros = Integer.MAX_VALUE;
        first = true;
        index = 0;
        current = 0;
        Arrays.fill(indices, 0);
    }

    /**
     * Adds a new long value to the series. Note, values must be inserted in order.
     *
     * @param value next floating point value in the series
     */
    public int addValue(long value) {
        if (first) {
            return writeFirst(value);
        } else {
            return compressValue(value);
        }
    }

    private int writeFirst(long value) {
        first = false;
        storedValues[current] = value;
        out.writeLong(storedValues[current], 64);
        indices[(int) value & setLsb] = index;
        return 64;
    }

    /**
     * Closes the block and writes the remaining stuff to the BitOutput.
     */
    @Override
    public int close() {
        int thisSize = addValue(Elf64Utils.END_SIGN);
        out.flush();
        return thisSize;
    }

    private int compressValue(long value) {
        int thisSize = 0;
        int key = (int) value & setLsb;
        long xor;
        int previousIndex;
        int trailingZeros = 0;
        int currIndex = indices[key];
        if ((index - currIndex) < previousValues) {
            long tempXor = value ^ storedValues[currIndex % previousValues];
            trailingZeros = Long.numberOfTrailingZeros(tempXor);
            if (trailingZeros > threshold) {
                previousIndex = currIndex % previousValues;
                xor = tempXor;
            } else {
                previousIndex = index % previousValues;
                xor = storedValues[previousIndex] ^ value;
            }
        } else {
            previousIndex = index % previousValues;
            xor = storedValues[previousIndex] ^ value;
        }

        if (xor == 0) {
            out.writeInt(previousIndex, this.flagZeroSize);
            thisSize += this.flagZeroSize;
            storedLeadingZeros = 65;
        } else {
            int leadingZeros = leadingRound[Long.numberOfLeadingZeros(xor)];

            if (trailingZeros > threshold) {
                int significantBits = 64 - leadingZeros - trailingZeros;
                out.writeInt(512 * (previousValues + previousIndex) + 64 * leadingRepresentation[leadingZeros] + significantBits, this.flagOneSize);
                out.writeLong(xor >>> trailingZeros, significantBits); // Store the meaningful bits of XOR
                thisSize += significantBits + this.flagOneSize;
                storedLeadingZeros = 65;
            } else if (leadingZeros == storedLeadingZeros) {
                out.writeInt(2, 2);
                int significantBits = 64 - leadingZeros;
                out.writeLong(xor, significantBits);
                thisSize += 2 + significantBits;
            } else {
                storedLeadingZeros = leadingZeros;
                int significantBits = 64 - leadingZeros;
                out.writeInt(24 + leadingRepresentation[leadingZeros], 5);
                out.writeLong(xor, significantBits);
                thisSize += 5 + significantBits;
            }
        }
        current = (current + 1) % previousValues;
        storedValues[current] = value;
        index++;
        indices[key] = index;
        return thisSize;
    }

}
