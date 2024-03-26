package org.urbcomp.startdb.selfstar.compressor32.xor;

import org.urbcomp.startdb.selfstar.utils.Elf32Utils;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.util.Arrays;

/**
 * Implements the Chimp128 time series compression. Value compression
 * is for floating points only.
 *
 * @author Panagiotis Liakos
 */
public class ChimpNXORCompressor32 implements IXORCompressor32 {

    public final static short[] leadingRepresentation = {0, 0, 0, 0, 0, 0, 0, 0,
            1, 1, 1, 1, 2, 2, 2, 2,
            3, 3, 4, 4, 5, 5, 6, 6,
            7, 7, 7, 7, 7, 7, 7, 7,
            7, 7, 7, 7, 7, 7, 7, 7,
            7, 7, 7, 7, 7, 7, 7, 7,
            7, 7, 7, 7, 7, 7, 7, 7,
            7, 7, 7, 7, 7, 7, 7, 7
    };
    public final static short[] leadingRound = {0, 0, 0, 0, 0, 0, 0, 0,
            8, 8, 8, 8, 12, 12, 12, 12,
            16, 16, 18, 18, 20, 20, 22, 22,
            24, 24, 24, 24, 24, 24, 24, 24,
            24, 24, 24, 24, 24, 24, 24, 24,
            24, 24, 24, 24, 24, 24, 24, 24,
            24, 24, 24, 24, 24, 24, 24, 24,
            24, 24, 24, 24, 24, 24, 24, 24
    };
    private final int[] storedValues;
    private final int threshold;
    private final int previousValues;
    private final int setLsb;
    private final int[] indices;
    private final int flagOneSize;
    private final int flagZeroSize;
    private final int capacity = 1000;
    private int storedLeadingZeros = Integer.MAX_VALUE;
    private boolean first = true;
    private OutputBitStream out;
    private int index = 0;
    private int current = 0;

    // We should have access to the series?
    public ChimpNXORCompressor32(int previousValues) {
        out = new OutputBitStream(
                new byte[(int) (((capacity + 1) * 4 + capacity / 4 + 1) * 1.2)]);
        this.previousValues = previousValues;
        int previousValuesLog2 = (int) (Math.log(previousValues) / Math.log(2));
        this.threshold = 5 + previousValuesLog2;
        this.setLsb = (int) Math.pow(2, threshold + 1) - 1;
        this.indices = new int[(int) Math.pow(2, threshold + 1)];
        this.storedValues = new int[previousValues];
        this.flagZeroSize = previousValuesLog2 + 2;
        this.flagOneSize = previousValuesLog2 + 10;
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
                new byte[(int) (((capacity + 1) * 4 + capacity / 4 + 1) * 1.2)]);
        storedLeadingZeros = Integer.MAX_VALUE;
        first = true;
        index = 0;
        current = 0;
        Arrays.fill(indices, 0);
    }

    /**
     * Adds a new int value to the series. Note, values must be inserted in order.
     *
     * @param value next floating point value in the series
     */
    public int addValue(int value) {
        if (first) {
            return writeFirst(value);
        } else {
            return compressValue(value);
        }
    }

    private int writeFirst(int value) {
        first = false;
        storedValues[current] = value;
        out.writeInt(storedValues[current], 32);
        indices[value & setLsb] = index;
        return 32;
    }

    /**
     * Closes the block and writes the remaining stuff to the BitOutput.
     */
    @Override
    public int close() {
        int thisSize = addValue(Elf32Utils.END_SIGN);
        out.flush();
        return thisSize;
    }

    private int compressValue(int value) {
        int thisSize = 0;
        int key = value & setLsb;
        int xor;
        int previousIndex;
        int trailingZeros = 0;
        int currIndex = indices[key];
        if ((index - currIndex) < previousValues) {
            int tempXor = value ^ storedValues[currIndex % previousValues];
            trailingZeros = Integer.numberOfTrailingZeros(tempXor);
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
            storedLeadingZeros = 33;
        } else {
            int leadingZeros = leadingRound[Integer.numberOfLeadingZeros(xor)];

            if (trailingZeros > threshold) {
                int significantBits = 32 - leadingZeros - trailingZeros;
                out.writeInt(256 * (previousValues + previousIndex) + 32 * leadingRepresentation[leadingZeros] + significantBits, this.flagOneSize);
                out.writeInt(xor >>> trailingZeros, significantBits); // Store the meaningful bits of XOR
                thisSize += significantBits + this.flagOneSize;
                storedLeadingZeros = 33;
            } else if (leadingZeros == storedLeadingZeros) {
                out.writeInt(2, 2);
                int significantBits = 32 - leadingZeros;
                out.writeInt(xor, significantBits);
                thisSize += 2 + significantBits;
            } else {
                storedLeadingZeros = leadingZeros;
                int significantBits = 32 - leadingZeros;
                out.writeInt(24 + leadingRepresentation[leadingZeros], 5);
                out.writeInt(xor, significantBits);
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
