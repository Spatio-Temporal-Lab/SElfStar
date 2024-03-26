package org.urbcomp.startdb.selfstar.compressor32.xor;

import org.urbcomp.startdb.selfstar.utils.*;

import java.util.Arrays;

public class SElfStarXORCompressor32 implements IXORCompressor32 {
    private final int[] leadingRepresentation = {
            0, 0, 0, 0, 0, 0, 0, 0,
            1, 1, 1, 1, 2, 2, 2, 2,
            3, 3, 3, 3, 3, 3, 3, 3,
            3, 3, 3, 3, 3, 3, 3, 3,
    };
    private final int[] leadingRound = {
            0, 0, 0, 0, 0, 0, 0, 0,
            8, 8, 8, 8, 12, 12, 12, 12,
            16, 16, 16, 16, 16, 16, 16, 16,
            16, 16, 16, 16, 16, 16, 16, 16
    };
    private final int[] trailingRepresentation = {
            0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,
            1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1
    };
    private final int[] trailingRound = {
            0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,
            16, 16, 16, 16, 16, 16, 16, 16,
            16, 16, 16, 16, 16, 16, 16, 16
    };
    private final int[] leadDistribution = new int[32];
    private final int[] trailDistribution = new int[32];
    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = Integer.MAX_VALUE;
    private int storedVal = 0;
    private boolean first = true;
    private int[] leadPositions = {0, 8, 12, 16};
    private int[] trailPositions = {0, 16};
    private boolean updatePositions = false;
    private boolean writePositions = false;
    private final OutputBitStream out;

    private int leadingBitsPerValue = 2;

    private int trailingBitsPerValue = 1;

    private int capacity = 1000;


    public SElfStarXORCompressor32(int window) {
        this.capacity = window;
        out = new OutputBitStream(
                new byte[(int) (((capacity + 1) * 4 + capacity / 4 + 1) * 1.2)]);

    }

    public SElfStarXORCompressor32() {
        out = new OutputBitStream(
                new byte[(int) (((capacity + 1) * 4 + capacity / 4 + 1) * 1.2)]);
    }

    @Override
    public OutputBitStream getOutputStream() {
        return this.out;
    }

    /**
     * Adds a new int value to the series. Note, values must be inserted in order.
     *
     * @param value next floating point value in the series
     */
    @Override
    public int addValue(int value) {
        if (first) {
            if (writePositions) {
                return out.writeBit(true)
                        + PostOfficeSolver32.writePositions(leadPositions, out)
                        + PostOfficeSolver32.writePositions(trailPositions, out)
                        + writeFirst(value);
            } else {
                return out.writeBit(false) + writeFirst(value);
            }
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
        if (updatePositions) {
            // we update distribution using the inner info
            leadPositions = PostOfficeSolver32.initRoundAndRepresentation(leadDistribution, leadingRepresentation, leadingRound);
            leadingBitsPerValue = PostOfficeSolver32.positionLength2Bits[leadPositions.length];

            trailPositions = PostOfficeSolver32.initRoundAndRepresentation(trailDistribution, trailingRepresentation, trailingRound);
            trailingBitsPerValue = PostOfficeSolver32.positionLength2Bits[trailPositions.length];
        }
        writePositions = updatePositions;
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
            leadDistribution[Integer.numberOfLeadingZeros(xor)]++;
            trailDistribution[Integer.numberOfTrailingZeros(xor)]++;

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
                            (((leadingRepresentation[storedLeadingZeros] << trailingBitsPerValue) |
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
        first = true;
        updatePositions = false;
        Arrays.fill(leadDistribution, 0);
        Arrays.fill(trailDistribution, 0);
    }

    @Override
    public void setDistribution(int[] leadDistributionIgnore, int[] trailDistributionIgnore) {
        this.updatePositions = true;
    }
}
