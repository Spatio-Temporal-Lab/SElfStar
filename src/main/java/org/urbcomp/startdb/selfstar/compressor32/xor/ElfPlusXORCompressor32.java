package org.urbcomp.startdb.selfstar.compressor32.xor;

import org.urbcomp.startdb.selfstar.utils.Elf32Utils;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

public class ElfPlusXORCompressor32 implements IXORCompressor32 {
    public final static short[] leadingRepresentation = {
            0, 0, 0, 0, 0, 0, 1, 1,
            1, 1, 2, 2, 3, 3, 4, 4,
            5, 5, 6, 6, 7, 7, 7, 7,
            7, 7, 7, 7, 7, 7, 7, 7
    };

    public final static short[] leadingRound = {
            0, 0, 0, 0, 0, 0, 6, 6,
            6, 6, 10, 10, 12, 12, 14, 14,
            16, 16, 18, 18, 20, 20, 20, 20,
            20, 20, 20, 20, 20, 20, 20, 20
    };
    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = Integer.MAX_VALUE;
    private int storedVal = 0;
    private boolean first = true;
    private final OutputBitStream out;

    private int capacity = 1000;

    public ElfPlusXORCompressor32() {
        out = new OutputBitStream(
                new byte[(int) (((capacity + 1) * 4 + capacity / 4 + 1) * 1.2)]);
    }

    public ElfPlusXORCompressor32(int capacity) {
        this.capacity = capacity;
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
            return writeFirst(value);
        } else {
            return compressValue(value);
        }
    }

    private int writeFirst(int value) {
        first = false;
        storedVal = value;
        int trailingZeros = Integer.numberOfTrailingZeros(value);
        out.writeInt(trailingZeros, 6);
        out.writeInt(storedVal >>> trailingZeros, 32 - trailingZeros);
        return 38 - trailingZeros;
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
            int trailingZeros = Integer.numberOfTrailingZeros(xor);

            if (leadingZeros == storedLeadingZeros && trailingZeros >= storedTrailingZeros) {
                // case 00
                int centerBits = 32 - storedLeadingZeros - storedTrailingZeros;
                int len = 2 + centerBits;
                if (len > 32) {
                    out.writeInt(0, 2);
                    out.writeInt(xor >>> storedTrailingZeros, centerBits);
                } else {
                    out.writeInt(xor >>> storedTrailingZeros, len);
                }
                thisSize += len;
            } else {
                storedLeadingZeros = leadingZeros;
                storedTrailingZeros = trailingZeros;
                int centerBits = 32 - storedLeadingZeros - storedTrailingZeros;

                if (centerBits <= 8) {
                    // case 10
                    out.writeInt((((0x2 << 3) | leadingRepresentation[storedLeadingZeros]) << 3) | (centerBits & 0x7), 8);
                    out.writeInt(xor >>> storedTrailingZeros, centerBits);

                    thisSize += 8 + centerBits;
                } else {
                    // case 11
                    out.writeInt((((0x3 << 3) | leadingRepresentation[storedLeadingZeros]) << 5) | (centerBits & 0x1f), 10);
                    out.writeInt(xor >>> storedTrailingZeros, centerBits);

                    thisSize += 10 + centerBits;
                }
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
    }
}
