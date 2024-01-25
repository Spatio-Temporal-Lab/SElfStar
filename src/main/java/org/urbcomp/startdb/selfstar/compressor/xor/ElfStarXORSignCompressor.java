package org.urbcomp.startdb.selfstar.compressor.xor;

import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;
import org.urbcomp.startdb.selfstar.utils.PostOfficeSolver;

import java.util.Arrays;

public class ElfStarXORSignCompressor implements IXORCompressor {
    private final int[] leadingRepresentation = new int[64];
    private final int[] leadingRound = new int[64];
    private final int[] trailingRepresentation = new int[64];
    private final int[] trailingRound = new int[64];
    private final long SIGN_MASK = 0x7fffffffffffffffL;
    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = Integer.MAX_VALUE;
    private long storedVal = 0;
    private boolean first = true;
    private int[] leadDistribution;
    private int[] trailDistribution;
    private OutputBitStream out;

    private int leadingBitsPerValue;

    private int trailingBitsPerValue;

    private int capacity = 1000;

    public ElfStarXORSignCompressor() {
        out = new OutputBitStream(
                new byte[(int) (((capacity + 1) * 8 + capacity / 8 + 1) * 1.2)]);
    }

    public ElfStarXORSignCompressor(int window) {
        this.capacity = window;
        out = new OutputBitStream(
                new byte[(int) (((capacity + 1) * 8 + capacity / 8 + 1) * 1.2)]);
    }

    @Override
    public OutputBitStream getOutputStream() {
        return this.out;
    }

    private int initLeadingRoundAndRepresentation(int[] distribution) {
        int[] positions = PostOfficeSolver.initRoundAndRepresentation(distribution, leadingRepresentation, leadingRound);
        leadingBitsPerValue = PostOfficeSolver.positionLength2Bits[positions.length];
        return PostOfficeSolver.writePositions(positions, out);
    }

    private int initTrailingRoundAndRepresentation(int[] distribution) {
        int[] positions = PostOfficeSolver.initRoundAndRepresentation(distribution, trailingRepresentation, trailingRound);
        trailingBitsPerValue = PostOfficeSolver.positionLength2Bits[positions.length];
        return PostOfficeSolver.writePositions(positions, out);
    }

    /**
     * Adds a new long value to the series. Note, values must be inserted in order.
     *
     * @param value next floating point value in the series
     */
    @Override
    public int addValue(long value) {
        if (first) {
            return initLeadingRoundAndRepresentation(leadDistribution)
                    + initTrailingRoundAndRepresentation(trailDistribution)
                    + writeFirst(value);
        } else {
            return compressValue(value);
        }
    }

    private int writeFirst(long value) {
        first = false;
        out.writeLong(value >>> 63, 1);
        storedVal = value & SIGN_MASK;
        int trailingZeros = Long.numberOfTrailingZeros(storedVal);
        out.writeInt(trailingZeros, 7);
        if (trailingZeros < 64) {
            out.writeLong(storedVal >>> (trailingZeros + 1), 62 - trailingZeros);
            return 70 - trailingZeros;
        } else {
            return 8;
        }
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
        out.writeLong(value >>> 63, 1);
        thisSize += 1;
        value = value & SIGN_MASK;
        long xor = storedVal ^ value;

        if (xor == 0) {
            // case 01
            out.writeInt(1, 2);
            thisSize += 2;
        } else {
            int leadingZeros = leadingRound[Long.numberOfLeadingZeros(xor)];
            int trailingZeros = trailingRound[Long.numberOfTrailingZeros(xor)];

            if (leadingZeros >= storedLeadingZeros && trailingZeros >= storedTrailingZeros &&
                    (leadingZeros - storedLeadingZeros) + (trailingZeros - storedTrailingZeros) < 1 + leadingBitsPerValue + trailingBitsPerValue) {
                // case 1
                int centerBits = 64 - storedLeadingZeros - storedTrailingZeros;
                int len = 1 + centerBits;
                if (len > 64) {
                    out.writeInt(1, 1);
                    out.writeLong(xor >>> storedTrailingZeros, centerBits);
                } else {
                    out.writeLong((1L << centerBits) | (xor >>> storedTrailingZeros), 1 + centerBits);
                }
                thisSize += len;
            } else {
                storedLeadingZeros = leadingZeros;
                storedTrailingZeros = trailingZeros;
                int centerBits = 64 - storedLeadingZeros - storedTrailingZeros;

                // case 00
                int len = 2 + leadingBitsPerValue + trailingBitsPerValue + centerBits;
                if (len > 64) {
                    out.writeInt((leadingRepresentation[storedLeadingZeros] << trailingBitsPerValue)
                            | trailingRepresentation[storedTrailingZeros], 2 + leadingBitsPerValue + trailingBitsPerValue);
                    out.writeLong(xor >>> storedTrailingZeros, centerBits);
                } else {
                    out.writeLong(
                            ((((long) leadingRepresentation[storedLeadingZeros] << trailingBitsPerValue) |
                                    trailingRepresentation[storedTrailingZeros]) << centerBits) | (xor >>> storedTrailingZeros),
                            len
                    );
                }
                thisSize += len;
            }
            storedVal = value;
        }
        return thisSize;
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
        storedTrailingZeros = Integer.MAX_VALUE;
        storedVal = 0;
        first = true;
        Arrays.fill(leadingRepresentation, 0);
        Arrays.fill(leadingRound, 0);
        Arrays.fill(trailingRepresentation, 0);
        Arrays.fill(trailingRound, 0);
    }

    @Override
    public void setDistribution(int[] leadDistribution, int[] trailDistribution) {
        this.leadDistribution = leadDistribution;
        this.trailDistribution = trailDistribution;
    }
}
