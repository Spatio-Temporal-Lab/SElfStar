package org.urbcomp.startdb.selfstar.compressor32.xor;

import org.urbcomp.startdb.selfstar.utils.Elf32Utils;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;
import org.urbcomp.startdb.selfstar.utils.PostOfficeSolver32;

import java.util.Arrays;

public class ElfStarXORCompressor32 implements IXORCompressor32 {
    private final int[] leadingRepresentation = new int[32];
    private final int[] leadingRound = new int[32];
    private final int[] trailingRepresentation = new int[32];
    private final int[] trailingRound = new int[32];
    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = Integer.MAX_VALUE;
    private int storedVal = 0;
    private boolean first = true;
    private int[] leadDistribution;
    private int[] trailDistribution;

    private final OutputBitStream out;

    private int leadingBitsPerValue;

    private int trailingBitsPerValue;

    private int capacity = 1000;

    public ElfStarXORCompressor32() {
        out = new OutputBitStream(
                new byte[(int) (((capacity + 1) * 4 + capacity / 4 + 1) * 1.2)]);
    }

    public ElfStarXORCompressor32(int window) {
        this.capacity = window;
        out = new OutputBitStream(
                new byte[(int) (((capacity + 1) * 4 + capacity / 4 + 1) * 1.2)]);
    }

    @Override
    public OutputBitStream getOutputStream() {
        return this.out;
    }

    private int initLeadingRoundAndRepresentation(int[] distribution) {
        int[] positions = PostOfficeSolver32.initRoundAndRepresentation(distribution, leadingRepresentation, leadingRound);
        leadingBitsPerValue = PostOfficeSolver32.positionLength2Bits[positions.length];
        return PostOfficeSolver32.writePositions(positions, out);
    }

    private int initTrailingRoundAndRepresentation(int[] distribution) {
        int[] positions = PostOfficeSolver32.initRoundAndRepresentation(distribution, trailingRepresentation, trailingRound);
        trailingBitsPerValue = PostOfficeSolver32.positionLength2Bits[positions.length];
        return PostOfficeSolver32.writePositions(positions, out);
    }

    /**
     * Adds a new long value to the series. Note, values must be inserted in order.
     *
     * @param value next floating point value in the series
     */
    @Override
    public int addValue(int value) {
        if (first) {
            return initLeadingRoundAndRepresentation(leadDistribution)
                    + initTrailingRoundAndRepresentation(trailDistribution)
                    + writeFirst(value);
        } else {
            return compressValue(value);
        }
    }

    private int writeFirst(int value) {
        first = false;
        storedVal = value;
        int trailingZeros = Integer.numberOfTrailingZeros(value);
        out.writeInt(trailingZeros, 6);
        if (trailingZeros < 32) {
            out.writeInt(storedVal >>> (trailingZeros + 1), 31 - trailingZeros);
            return 37 - trailingZeros;
        } else {
            return 6;
        }
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
        int xor = storedVal ^ value;

        if (xor == 0) {
            // case 01
            out.writeInt(1, 2);
            thisSize += 2;
        } else {
            int leadingZeros = leadingRound[Integer.numberOfLeadingZeros(xor)];
            int trailingZeros = trailingRound[Integer.numberOfTrailingZeros(xor)];

            if (leadingZeros >= storedLeadingZeros && trailingZeros >= storedTrailingZeros &&
                    (leadingZeros - storedLeadingZeros) + (trailingZeros - storedTrailingZeros) < 1 + leadingBitsPerValue + trailingBitsPerValue) {
                // case 1
                int centerBits = 32 - storedLeadingZeros - storedTrailingZeros;
                int len = 1 + centerBits;
                if (len > 32) {
                    out.writeInt(1, 1);
                    out.writeInt(xor >>> storedTrailingZeros, centerBits);
                } else {
                    out.writeInt((1 << centerBits) | (xor >>> storedTrailingZeros), 1 + centerBits);
                }
                thisSize += len;
            } else {
                storedLeadingZeros = leadingZeros;
                storedTrailingZeros = trailingZeros;
                int centerBits = 32 - storedLeadingZeros - storedTrailingZeros;

                // case 00
                int len = 2 + leadingBitsPerValue + trailingBitsPerValue + centerBits;
                if (len > 32) {
                    out.writeInt((leadingRepresentation[storedLeadingZeros] << trailingBitsPerValue)
                            | trailingRepresentation[storedTrailingZeros], 2 + leadingBitsPerValue + trailingBitsPerValue);
                    out.writeInt(xor >>> storedTrailingZeros, centerBits);
                } else {
                    out.writeInt(
                            ((( leadingRepresentation[storedLeadingZeros] << trailingBitsPerValue) |
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
        out.refresh();
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
