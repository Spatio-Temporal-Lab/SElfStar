package org.urbcomp.startdb.selfstar.decompressor32.xor;

import org.urbcomp.startdb.selfstar.utils.Elf32Utils;
import org.urbcomp.startdb.selfstar.utils.InputBitStream;

import java.io.IOException;

public class ChimpNXORDecompressor32 implements IXORDecompressor32 {

    public final static short[] leadingRepresentation = {0, 8, 12, 16, 18, 20, 22, 24};
    private final int[] storedValues;
    private final int previousValues;
    private final int previousValuesLog2;
    private final int initialFill;
    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedVal = 0;
    private int current = 0;
    private boolean first = true;
    private boolean endOfStream = false;
    private InputBitStream in;


    public ChimpNXORDecompressor32(int previousValues) {
        this(new byte[0], previousValues);
    }

    public ChimpNXORDecompressor32(byte[] bs, int previousValues) {
        in = new InputBitStream(bs);
        this.previousValues = previousValues;
        this.previousValuesLog2 = (int) (Math.log(previousValues) / Math.log(2));
        this.initialFill = previousValuesLog2 + 8;
        this.storedValues = new int[previousValues];
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
        current = 0;
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
            storedVal = in.readInt(32);
            storedValues[current] = storedVal;
            if (storedValues[current] == Elf32Utils.END_SIGN) {
                endOfStream = true;
            }
        } else {
            nextValue();
        }
    }

    private void nextValue() throws IOException {
        // Read value
        int flag = in.readInt(2);
        int value;
        switch (flag) {
            case 3:
                storedLeadingZeros = leadingRepresentation[in.readInt(3)];
                value = in.readInt(32 - storedLeadingZeros);
                value = storedVal ^ value;

                if (value == Elf32Utils.END_SIGN) {
                    endOfStream = true;
                    return;
                } else {
                    storedVal = value;
                    current = (current + 1) % previousValues;
                    storedValues[current] = storedVal;
                }
                break;
            case 2:
                value = in.readInt(32 - storedLeadingZeros);
                value = storedVal ^ value;
                if (value == Elf32Utils.END_SIGN) {
                    endOfStream = true;
                    return;
                } else {
                    storedVal = value;
                    current = (current + 1) % previousValues;
                    storedValues[current] = storedVal;
                }
                break;
            case 1:
                int fill = this.initialFill;
                int temp = in.readInt(fill);
                int index = temp >>> (fill -= previousValuesLog2) & (1 << previousValuesLog2) - 1;
                storedLeadingZeros = leadingRepresentation[temp >>> (fill -= 3) & (1 << 3) - 1];
                int significantBits = temp >>> (fill - 5) & (1 << 5) - 1;
                storedVal = storedValues[index];
                if (significantBits == 0) {
                    significantBits = 32;
                }
                int storedTrailingZeros = 32 - significantBits - storedLeadingZeros;
                value = in.readInt(32 - storedLeadingZeros - storedTrailingZeros);
                value <<= storedTrailingZeros;
                value = storedVal ^ value;
                if (value == Elf32Utils.END_SIGN) {
                    endOfStream = true;
                    return;
                } else {
                    storedVal = value;
                    current = (current + 1) % previousValues;
                    storedValues[current] = storedVal;
                }
                break;
            default:
                // else -> same value as before
                storedVal = storedValues[in.readInt(previousValuesLog2)];
                current = (current + 1) % previousValues;
                storedValues[current] = storedVal;
                break;
        }
    }

}
