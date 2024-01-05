package org.urbcomp.startdb.selfstar.decompressor32.xor;

import org.urbcomp.startdb.selfstar.utils.Elf32Utils;
import org.urbcomp.startdb.selfstar.utils.InputBitStream;

import java.io.IOException;

public class GorillaXORDecompressor32 implements IXORDecompressor32 {

    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = 0;
    private int storedVal = 0;
    private boolean first = true;
    private boolean endOfStream = false;

    private InputBitStream in;

    public GorillaXORDecompressor32() {
        this(new byte[0]);
    }

    public GorillaXORDecompressor32(byte[] bs) {
        in = new InputBitStream(bs);
    }

    @Override
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


    private void next() throws IOException {
        if (first) {
            first = false;
            storedVal = in.readInt(32);
            endOfStream = storedVal == Elf32Utils.END_SIGN;
        } else {
            nextValue();
        }
    }

    private void nextValue() throws IOException {
        // Read value
        if (in.readBit() == 1) {
            // else -> same value as before
            if (in.readBit() == 1) {
                // New leading and trailing zeros
                storedLeadingZeros = in.readInt(4);

                int significantBits = in.readInt(5);
                if (significantBits == 0) {
                    significantBits = 32;
                }
                storedTrailingZeros = 32 - significantBits - storedLeadingZeros;
            }
            int value = in.readInt(32 - storedLeadingZeros - storedTrailingZeros);
            value <<= storedTrailingZeros;
            value = storedVal ^ value;
            endOfStream = value == Elf32Utils.END_SIGN;
            storedVal = value;
        }
    }
}
