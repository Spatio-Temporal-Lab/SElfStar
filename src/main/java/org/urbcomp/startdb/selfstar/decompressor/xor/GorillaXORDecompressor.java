package org.urbcomp.startdb.selfstar.decompressor.xor;

import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.InputBitStream;

import java.io.IOException;

public class GorillaXORDecompressor implements IXORDecompressor {

    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = 0;
    private long storedVal = 0;
    private boolean first = true;
    private boolean endOfStream = false;

    private InputBitStream in;

    public GorillaXORDecompressor() {
        this(new byte[0]);
    }

    public GorillaXORDecompressor(byte[] bs) {
        in = new InputBitStream(bs);
    }

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
            storedVal = in.readLong(64);
            if (storedVal == Elf64Utils.END_SIGN) {
                endOfStream = true;
            }

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
                storedLeadingZeros = in.readInt(5);

                int significantBits = in.readInt(6);
                if (significantBits == 0) {
                    significantBits = 64;
                }
                storedTrailingZeros = 64 - significantBits - storedLeadingZeros;
            }
            long value = in.readLong(64 - storedLeadingZeros - storedTrailingZeros);
            value <<= storedTrailingZeros;
            value = storedVal ^ value;
            if (value == Elf64Utils.END_SIGN) {
                endOfStream = true;
            } else {
                storedVal = value;
            }
        }
    }
}
