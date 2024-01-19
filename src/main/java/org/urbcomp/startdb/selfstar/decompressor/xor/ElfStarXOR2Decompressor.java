package org.urbcomp.startdb.selfstar.decompressor.xor;

import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.InputBitStream;
import org.urbcomp.startdb.selfstar.utils.PostOfficeSolver;

import java.io.IOException;
import java.util.Arrays;

public class ElfStarXOR2Decompressor implements IXORDecompressor {
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

    public ElfStarXOR2Decompressor() {
    }


    private void initLeadingRepresentation() {
        try {
            int num = in.readBufferInt(5);
            if (num == 0) {
                num = 32;
            }
            leadingBitsPerValue = PostOfficeSolver.positionLength2Bits[num];
            leadingRepresentation = new int[num];
            for (int i = 0; i < num; i++) {
                leadingRepresentation[i] = in.readBufferInt(6);
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private void initTrailingRepresentation() {
        try {
            int num = in.readBufferInt(5);
            if (num == 0) {
                num = 32;
            }
            trailingBitsPerValue = PostOfficeSolver.positionLength2Bits[num];
            trailingRepresentation = new int[num];
            for (int i = 0; i < num; i++) {
                trailingRepresentation[i] = in.readBufferInt(6);
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
        storedVal = 0;
        storedLeadingZeros = Integer.MAX_VALUE;
        storedTrailingZeros = Integer.MAX_VALUE;
        first = true;
        endOfStream = false;

        Arrays.fill(trailingRepresentation, 0);
        trailingBitsPerValue = 0;
        Arrays.fill(leadingRepresentation, 0);
        leadingBitsPerValue = 0;
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
            int trailingZeros = in.readBufferInt(7);
            if (trailingZeros < 64) {
                storedVal = ((in.readBufferLong(63 - trailingZeros) << 1) + 1) << trailingZeros;
            } else {
                storedVal = 0;
            }
            if (storedVal == Elf64Utils.END_SIGN) {
                endOfStream = true;
            }
        } else {
            nextValue();
        }
    }

    private void nextValue() throws IOException {
        long value;
        int centerBits;

        if (in.readBufferInt(1) == 1) {
            // case 1
            centerBits = 64 - storedLeadingZeros - storedTrailingZeros;
            value = in.readBufferLong(centerBits) << storedTrailingZeros;
            value = storedVal ^ value;
            if (value == Elf64Utils.END_SIGN) {
                endOfStream = true;
            } else {
                storedVal = value;
            }
        } else if (in.readBufferInt(1) == 0) {
            // case 00
            int leadAndTrail = in.readBufferInt(leadingBitsPerValue + trailingBitsPerValue);
            int lead = leadAndTrail >>> trailingBitsPerValue;
            int trail = ~(0xffffffff << trailingBitsPerValue) & leadAndTrail;
            storedLeadingZeros = leadingRepresentation[lead];
            storedTrailingZeros = trailingRepresentation[trail];
            centerBits = 64 - storedLeadingZeros - storedTrailingZeros;

            value = in.readBufferLong(centerBits) << storedTrailingZeros;
            value = storedVal ^ value;
            if (value == Elf64Utils.END_SIGN) {
                endOfStream = true;
            } else {
                storedVal = value;
            }
        }
    }
}
