package org.urbcomp.startdb.selfstar.decompressor32.xor;

import org.urbcomp.startdb.selfstar.utils.Elf32Utils;
import org.urbcomp.startdb.selfstar.utils.InputBitStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElfXORDecompressor32 implements IXORDecompressor32 {
    private final static short[] leadingRepresentation = {0, 6, 10, 12, 14, 16, 18, 20};
    private int storedVal = 0;
    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = Integer.MAX_VALUE;
    private boolean first = true;
    private boolean endOfStream = false;
    private InputBitStream in;

    public ElfXORDecompressor32() {
        this(new byte[0]);
    }

    public ElfXORDecompressor32(byte[] bs) {
        in = new InputBitStream(bs);
    }

    public List<Float> getValues() {
        List<Float> list = new ArrayList<>(1024);
        Float value = readValue();
        while (value != null) {
            list.add(value);
            value = readValue();
        }
        return list;
    }

    public InputBitStream getInputStream() {
        return in;
    }

    @Override
    public void setBytes(byte[] bs) {
        in = new InputBitStream(bs);
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
    public Float readValue() {
        try {
            next();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
        if (endOfStream) {
            return null;
        }
        return Float.intBitsToFloat(storedVal);
    }

    private void next() throws IOException {
        if (first) {
            first = false;
            int trailingZeros = in.readInt(6);
            storedVal = in.readInt(32 - trailingZeros) << trailingZeros;
            endOfStream = storedVal == Elf32Utils.END_SIGN;
        } else {
            nextValue();
        }
    }

    private void nextValue() throws IOException {
        int value;
        int centerBits, leadAndCenter;
        int flag = in.readInt(2);
        switch (flag) {
            case 3:
                // case 11
                leadAndCenter = in.readInt(8);
                storedLeadingZeros = leadingRepresentation[leadAndCenter >>> 5];
                centerBits = leadAndCenter & 0x1f;
                if (centerBits == 0) {
                    centerBits = 32;
                }
                storedTrailingZeros = 32 - storedLeadingZeros - centerBits;
                value = in.readInt(centerBits) << storedTrailingZeros;
                value = storedVal ^ value;
                endOfStream = value == Elf32Utils.END_SIGN;
                storedVal = value;
                break;
            case 2:
                // case 10
                leadAndCenter = in.readInt(6);
                storedLeadingZeros = leadingRepresentation[leadAndCenter >>> 3];
                centerBits = leadAndCenter & 0x7;
                if (centerBits == 0) {
                    centerBits = 8;
                }
                storedTrailingZeros = 32 - storedLeadingZeros - centerBits;
                value = in.readInt(centerBits) << storedTrailingZeros;
                value = storedVal ^ value;
                endOfStream = value == Elf32Utils.END_SIGN;
                storedVal = value;
                break;
            case 1:
                // case 01, we do nothing, the same value as before
                break;
            default:
                // case 00
                centerBits = 32 - storedLeadingZeros - storedTrailingZeros;
                value = in.readInt(centerBits) << storedTrailingZeros;
                value = storedVal ^ value;
                endOfStream = value == Elf32Utils.END_SIGN;
                storedVal = value;
                break;
        }
    }
}
