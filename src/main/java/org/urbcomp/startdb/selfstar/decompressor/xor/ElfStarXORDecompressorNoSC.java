package org.urbcomp.startdb.selfstar.decompressor.xor;

import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.InputBitStream;
import org.urbcomp.startdb.selfstar.utils.PostOfficeSolver;

import java.io.IOException;

public class ElfStarXORDecompressorNoSC implements IXORDecompressor {
    private long storedVal = 0;
    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = Integer.MAX_VALUE;
    private boolean first = true;
    private boolean endOfStream = false;

    private InputBitStream in;

    private int[] leadingRepresentation;

    private int leadingBitsPerValue;

    public ElfStarXORDecompressorNoSC() {
    }

    private void initLeadingRepresentation() {
        try {
            int num = in.readInt(5);
            if (num == 0) {
                num = 32;
            }
            leadingRepresentation = new int[num];
            leadingBitsPerValue = PostOfficeSolver.positionLength2Bits[num];
            for (int i = 0; i < num; i++) {
                leadingRepresentation[i] = in.readInt(6);
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
            first = false;
            int trailingZeros = in.readInt(7);
            storedVal = in.readLong(64 - trailingZeros) << trailingZeros;
            endOfStream = storedVal == Elf64Utils.END_SIGN;
        } else {
            nextValue();
        }
    }

    private void nextValue() throws IOException {
        long value;
        int centerBits, leadAndCenter;
        int flag = in.readInt(2);
        switch (flag) {
            case 3:
                // case 11
                leadAndCenter = in.readInt(leadingBitsPerValue + 6);
                storedLeadingZeros = leadingRepresentation[leadAndCenter >>> 6];
                centerBits = leadAndCenter & 0x3f;
                if (centerBits == 0) {
                    centerBits = 64;
                }
                storedTrailingZeros = 64 - storedLeadingZeros - centerBits;
                value = in.readLong(centerBits) << storedTrailingZeros;
                value = storedVal ^ value;
                endOfStream = value == Elf64Utils.END_SIGN;
                storedVal = value;
                break;
            case 2:
                // case 10
                leadAndCenter = in.readInt(leadingBitsPerValue + 4);
                storedLeadingZeros = leadingRepresentation[leadAndCenter >>> 4];
                centerBits = leadAndCenter & 0xf;
                if (centerBits == 0) {
                    centerBits = 16;
                }
                storedTrailingZeros = 64 - storedLeadingZeros - centerBits;
                value = in.readLong(centerBits) << storedTrailingZeros;
                value = storedVal ^ value;
                endOfStream = value == Elf64Utils.END_SIGN;
                storedVal = value;
                break;
            case 1:
                // case 01, we do nothing, the same value as before
                break;
            default:
                // case 00
                centerBits = 64 - storedLeadingZeros - storedTrailingZeros;
                value = in.readLong(centerBits) << storedTrailingZeros;
                value = storedVal ^ value;
                endOfStream = value == Elf64Utils.END_SIGN;
                storedVal = value;
                break;
        }
    }
}
