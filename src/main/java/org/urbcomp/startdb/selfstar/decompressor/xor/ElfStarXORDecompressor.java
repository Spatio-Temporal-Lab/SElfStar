package org.urbcomp.startdb.selfstar.decompressor.xor;

import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.InputBitStream;
import org.urbcomp.startdb.selfstar.utils.PostOfficeSolver;

import java.io.IOException;

public class ElfStarXORDecompressor implements IXORDecompressor {
    private long storedVal = 0;
    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = Integer.MAX_VALUE;
    private boolean first = true;
    private boolean endOfStream = false;

    private InputBitStream in;

    private int[] leadingRepresentation;

    private int[] trailingRepresentation;

    private int leadingBitsPerValue;

    private int trailingBitsPerValue;

    public ElfStarXORDecompressor() {
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

    private void initTrailingRepresentation() {
        try {
            int num = in.readInt(5);
            if (num == 0) {
                num = 32;
            }
            trailingBitsPerValue = PostOfficeSolver.positionLength2Bits[num];
            trailingRepresentation = new int[num];
            for (int i = 0; i < num; i++) {
                trailingRepresentation[i] = in.readInt(6);
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void setBytes(byte[] bs) {
        in = new InputBitStream(bs);
    }

    @Override
    public InputBitStream getInputStream() {
        return in;
    }

    @Override
    public void refresh() {
        first = true;
        endOfStream = false;
    }

    /**
     * Returns the next pair in the time series, if available.
     *
     * @return Pair if there's next value, null if series is done.
     */
    @Override
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
            initTrailingRepresentation();
            first = false;
            int trailingZeros = in.readInt(7);
            if (trailingZeros < 64) {
                storedVal = ((in.readLong(63 - trailingZeros) << 1) + 1) << trailingZeros;
            } else {
                storedVal = 0;
            }
            endOfStream = storedVal == Elf64Utils.END_SIGN;
        } else {
            nextValue();
        }
    }

    private void nextValue() throws IOException {
        long value;
        int centerBits;

        if (in.readInt(1) == 1) {
            // case 1
            centerBits = 64 - storedLeadingZeros - storedTrailingZeros;
            value = in.readLong(centerBits) << storedTrailingZeros;
            value = storedVal ^ value;
            endOfStream = value == Elf64Utils.END_SIGN;
            storedVal = value;
        } else if (in.readInt(1) == 0) {
            // case 00
            int leadAndTrail = in.readInt(leadingBitsPerValue + trailingBitsPerValue);
            int lead = leadAndTrail >>> trailingBitsPerValue;
            int trail = ~(0xffffffff << trailingBitsPerValue) & leadAndTrail;
            storedLeadingZeros = leadingRepresentation[lead];
            storedTrailingZeros = trailingRepresentation[trail];
            centerBits = 64 - storedLeadingZeros - storedTrailingZeros;

            value = in.readLong(centerBits) << storedTrailingZeros;
            value = storedVal ^ value;
            endOfStream = value == Elf64Utils.END_SIGN;
            storedVal = value;
        }
    }
}
