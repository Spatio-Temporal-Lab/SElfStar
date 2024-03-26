package org.urbcomp.startdb.selfstar.compressor.xor;

import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;
import org.urbcomp.startdb.selfstar.utils.PostOfficeSolver;

import java.util.Arrays;

/**
 * Implements the Chimp128 time series compression. Value compression
 * is for floating points only.
 *
 * @author Panagiotis Liakos
 */
public class ChimpNAdaXORCompressor implements IXORCompressor {

    private final int[] leadingRepresentation = {0, 0, 0, 0, 0, 0, 0, 0,
            1, 1, 1, 1, 2, 2, 2, 2,
            3, 3, 4, 4, 5, 5, 6, 6,
            7, 7, 7, 7, 7, 7, 7, 7,
            7, 7, 7, 7, 7, 7, 7, 7,
            7, 7, 7, 7, 7, 7, 7, 7,
            7, 7, 7, 7, 7, 7, 7, 7,
            7, 7, 7, 7, 7, 7, 7, 7
    };
    private final int[] leadingRound = {0, 0, 0, 0, 0, 0, 0, 0,
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
    private final int[] leadDistribution = new int[64];
    private int storedLeadingZeros = Integer.MAX_VALUE;
    private boolean first = true;
    private final OutputBitStream out;
    private int index = 0;
    private int current = 0;
    private int[] leadPositions = {0, 8, 12, 16, 18, 20, 22, 24};
    private boolean updatePositions = false;
    private int leadingBitsPerValue = 3;


    // We should have access to the series?
    public ChimpNAdaXORCompressor(int previousValues) {
        int capacity = 1000;
        out = new OutputBitStream(
                new byte[(int) (((capacity + 1) * 8 + capacity / 8 + 1) * 1.2)]);
        this.previousValues = previousValues;
        int previousValuesLog2 = (int) (Math.log(previousValues) / Math.log(2));
        this.threshold = 6 + previousValuesLog2;
        this.setLsb = (int) Math.pow(2, threshold + 1) - 1;
        this.indices = new int[(int) Math.pow(2, threshold + 1)];
        this.storedValues = new long[previousValues];
        this.flagZeroSize = previousValuesLog2 + 2;
        this.flagOneSize = previousValuesLog2 + 8;
    }

    public OutputBitStream getOutputStream() {
        return out;
    }


    /**
     * Adds a new long value to the series. Note, values must be inserted in order.
     *
     * @param value next floating point value in the series
     */
    public int addValue(long value) {
        if (first) {
            return PostOfficeSolver.writePositions(leadPositions, out) + writeFirst(value);
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
        if (updatePositions) {
            // we update distribution using the inner info
            leadPositions = PostOfficeSolver.initRoundAndRepresentation(leadDistribution, leadingRepresentation, leadingRound);
            leadingBitsPerValue = PostOfficeSolver.positionLength2Bits[leadPositions.length];
        }
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
            leadDistribution[Long.numberOfLeadingZeros(xor)]++;

            if (trailingZeros > threshold) {
                int significantBits = 64 - leadingZeros - trailingZeros;
                out.writeInt(((previousValues + previousIndex) << (leadingBitsPerValue + 6)) | (leadingRepresentation[leadingZeros] << 6) | significantBits, this.flagOneSize + leadingBitsPerValue);
                out.writeLong(xor >>> trailingZeros, significantBits); // Store the meaningful bits of XOR
                thisSize += significantBits + this.flagOneSize + leadingBitsPerValue;
                storedLeadingZeros = 65;
            } else if (leadingZeros == storedLeadingZeros) {
                out.writeInt(2, 2);
                int significantBits = 64 - leadingZeros;
                out.writeLong(xor, significantBits);
                thisSize += 2 + significantBits;
            } else {
                storedLeadingZeros = leadingZeros;
                int significantBits = 64 - leadingZeros;
                out.writeInt(3 << leadingBitsPerValue | leadingRepresentation[leadingZeros], 2 + leadingBitsPerValue);

                out.writeLong(xor, significantBits);
                thisSize += 2 + leadingBitsPerValue + significantBits;
            }
        }
        current = (current + 1) % previousValues;
        storedValues[current] = value;
        index++;
        indices[key] = index;
        return thisSize;
    }


    @Override
    public byte[] getOut() {
        return out.getBuffer();
    }

    @Override
    public void refresh() {
        out.refresh();
        storedLeadingZeros = Integer.MAX_VALUE;
        first = true;
        index = 0;
        current = 0;
        updatePositions = false;
        Arrays.fill(storedValues, 0);
        Arrays.fill(leadDistribution, 0);
        Arrays.fill(indices, 0);
    }

    @Override
    public void setDistribution(int[] leadDistributionIgnore, int[] trailDistributionIgnore) {
        this.updatePositions = true;
    }

}
