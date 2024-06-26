package org.urbcomp.startdb.selfstar.compressor.xor;

import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;
import org.urbcomp.startdb.selfstar.utils.PostOfficeSolver;

import java.util.Arrays;

/**
 * Implements the Chimp time series compression. Value compression
 * is for floating points only.
 *
 * @author Panagiotis Liakos
 */
public class ChimpAdaXORCompressor implements IXORCompressor {

    private final static int THRESHOLD = 6;
    private final int[] leadingRepresentation = {
        0, 0, 0, 0, 0, 0, 0, 0,
        1, 1, 1, 1, 2, 2, 2, 2,
        3, 3, 4, 4, 5, 5, 6, 6,
        7, 7, 7, 7, 7, 7, 7, 7,
        7, 7, 7, 7, 7, 7, 7, 7,
        7, 7, 7, 7, 7, 7, 7, 7,
        7, 7, 7, 7, 7, 7, 7, 7,
        7, 7, 7, 7, 7, 7, 7, 7
    };
    private final int[] leadingRound = {
        0, 0, 0, 0, 0, 0, 0, 0,
        8, 8, 8, 8, 12, 12, 12, 12,
        16, 16, 18, 18, 20, 20, 22, 22,
        24, 24, 24, 24, 24, 24, 24, 24,
        24, 24, 24, 24, 24, 24, 24, 24,
        24, 24, 24, 24, 24, 24, 24, 24,
        24, 24, 24, 24, 24, 24, 24, 24,
        24, 24, 24, 24, 24, 24, 24, 24
    };
    private final int[] leadDistribution = new int[64];
    private int storedLeadingZeros = Integer.MAX_VALUE;
    private long storedVal = 0;
    private boolean first = true;
    private int[] leadPositions = {0, 8, 12, 16, 18, 20, 22, 24};
    private boolean updatePositions = false;
    private final OutputBitStream out;

    private int leadingBitsPerValue = 3;

    // We should have access to the series?
    public ChimpAdaXORCompressor() {
        int capacity = 1000;
        out = new OutputBitStream(
                new byte[(int) (((capacity + 1) * 8 + capacity / 8 + 1) * 1.2)]);
    }

    @Override
    public OutputBitStream getOutputStream() {
        return this.out;
    }

    /**
     * Adds a new long value to the series. Note, values must be inserted in order.
     *
     * @param value next floating point value in the series
     */
    @Override
    public int addValue(long value) {
        if (first) {
            return PostOfficeSolver.writePositions(leadPositions, out) + writeFirst(value);
        } else {
            return compressValue(value);
        }
    }

    private int writeFirst(long value) {
        first = false;
        storedVal = value;
        out.writeLong(storedVal, 64);
        return 64;
    }

    /**
     * Closes the block and writes the remaining stuff to the BitOutput.
     */
    @Override
    public int close() {
        int thisSize = addValue(Elf64Utils.END_SIGN) + out.writeBit(false);
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
        long xor = storedVal ^ value;
        if (xor == 0) {
            // Write 0
            out.writeInt(0,2);
            thisSize += 2;
            storedLeadingZeros = 65;
        } else {
            int leadingZeros = leadingRound[Long.numberOfLeadingZeros(xor)];
            int trailingZeros = Long.numberOfTrailingZeros(xor);
            leadDistribution[Long.numberOfLeadingZeros(xor)]++;

            if (trailingZeros > THRESHOLD) {
                int significantBits = 64 - leadingZeros - trailingZeros;
                out.writeInt(1,2);
                out.writeInt(leadingRepresentation[leadingZeros], leadingBitsPerValue);
                out.writeInt(significantBits, 6);
                out.writeLong(xor >>> trailingZeros, significantBits); // Store the meaningful bits of XOR
                thisSize += 8 + leadingBitsPerValue + significantBits;
                storedLeadingZeros = 65;
            } else if (leadingZeros == storedLeadingZeros) {
                out.writeInt(2,2);
                int significantBits = 64 - leadingZeros;
                out.writeLong(xor, significantBits);
                thisSize += 2 + significantBits;
            } else {
                storedLeadingZeros = leadingZeros;
                int significantBits = 64 - leadingZeros;
                out.writeInt(3,2);
                out.writeInt(leadingRepresentation[leadingZeros], leadingBitsPerValue);
                out.writeLong(xor, significantBits);
                thisSize += 2 + leadingBitsPerValue + significantBits;
            }
        }
        storedVal = value;
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
        storedVal = 0;
        first = true;
        updatePositions = false;
        Arrays.fill(leadDistribution, 0);
    }

    @Override
    public void setDistribution(int[] leadDistributionIgnore, int[] trailDistributionIgnore) {
        this.updatePositions = true;
    }
}
