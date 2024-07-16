package org.urbcomp.startdb.selfstar.compressor.xor;

import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;
import org.urbcomp.startdb.selfstar.utils.PostOfficeSolver;

import java.util.Arrays;

public class ElfStarXORCompressorNoSC implements IXORCompressor {
    private final int[] leadingRepresentation = new int[64];
    private final int[] leadingRound = new int[64];
    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = Integer.MAX_VALUE;
    private long storedVal = 0;
    private boolean first = true;
    private int[] leadDistribution;
    private final OutputBitStream out;
    private int leadingBitsPerValue;

    public ElfStarXORCompressorNoSC() {
        int capacity = 1000;
        out = new OutputBitStream(
                new byte[(int) (((capacity + 1) * 8 + capacity / 8 + 1) * 1.2)]);
    }

    private int initLeadingRoundAndRepresentation(int[] distribution) {
        int[] positions = PostOfficeSolver.initRoundAndRepresentation(distribution, leadingRepresentation, leadingRound);
        leadingBitsPerValue = PostOfficeSolver.positionLength2Bits[positions.length];
        return PostOfficeSolver.writePositions(positions, out);
    }

    @Override
    public OutputBitStream getOutputStream() {
        return this.out;
    }

    @Override
    public int addValue(long value) {
        if (first) {
            return initLeadingRoundAndRepresentation(leadDistribution) + writeFirst(value);
        } else {
            return compressValue(value);
        }
    }

    private int writeFirst(long value) {
        first = false;
        storedVal = value;
        int trailingZeros = Long.numberOfTrailingZeros(value);
        out.writeInt(trailingZeros, 7);
        out.writeLong(storedVal >>> trailingZeros, 64 - trailingZeros);
        return 71 - trailingZeros;
    }

    private int compressValue(long value) {
        int thisSize = 0;
        long xor = storedVal ^ value;

        if (xor == 0) {
            // case 01
            out.writeInt(1, 2);
            thisSize += 2;
        } else {
            int leadingZeros = leadingRound[Long.numberOfLeadingZeros(xor)];
            int trailingZeros = Long.numberOfTrailingZeros(xor);

            if (leadingZeros == storedLeadingZeros && trailingZeros >= storedTrailingZeros) {
                // case 00
                int centerBits = 64 - storedLeadingZeros - storedTrailingZeros;
                int len = 2 + centerBits;
                if (len > 64) {
                    out.writeInt(0, 2);
                    out.writeLong(xor >>> storedTrailingZeros, centerBits);
                } else {
                    out.writeLong(xor >>> storedTrailingZeros, len);
                }

                thisSize += len;
            } else {
                storedLeadingZeros = leadingZeros;
                storedTrailingZeros = trailingZeros;
                int centerBits = 64 - storedLeadingZeros - storedTrailingZeros;

                if (centerBits <= 16) {
                    // case 10
                    out.writeInt((((0x2 << leadingBitsPerValue) | leadingRepresentation[storedLeadingZeros]) << 4)
                            | (centerBits & 0xf), 6 + leadingBitsPerValue);
                    out.writeLong(xor >>> storedTrailingZeros, centerBits);

                    thisSize += 6 + leadingBitsPerValue + centerBits;
                } else {
                    // case 11
                    out.writeInt((((0x3 << leadingBitsPerValue) | leadingRepresentation[storedLeadingZeros]) << 6)
                            | (centerBits & 0x3f), 8 + leadingBitsPerValue);
                    out.writeLong(xor >>> storedTrailingZeros, centerBits);

                    thisSize += 8 + leadingBitsPerValue + centerBits;
                }
            }

            storedVal = value;
        }

        return thisSize;
    }

    @Override
    public int close() {
        int thisSize = addValue(Elf64Utils.END_SIGN);
        out.flush();
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
    }

    @Override
    public void setDistribution(int[] leadDistribution, int[] trailDistribution) {
        this.leadDistribution = leadDistribution;
    }
}