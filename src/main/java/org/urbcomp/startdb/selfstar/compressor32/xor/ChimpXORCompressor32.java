package org.urbcomp.startdb.selfstar.compressor32.xor;

import org.urbcomp.startdb.selfstar.utils.Elf32Utils;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

/**
 * Implements the Chimp time series compression. Value compression
 * is for floating points only.
 *
 * @author Panagiotis Liakos
 */
public class ChimpXORCompressor32 implements IXORCompressor32 {

    public final static int THRESHOLD = 5;
    public final static short[] leadingRepresentation = {0, 0, 0, 0, 0, 0, 0, 0,
            1, 1, 1, 1, 2, 2, 2, 2,
            3, 3, 4, 4, 5, 5, 6, 6,
            7, 7, 7, 7, 7, 7, 7, 7,
            7, 7, 7, 7, 7, 7, 7, 7,
            7, 7, 7, 7, 7, 7, 7, 7,
            7, 7, 7, 7, 7, 7, 7, 7,
            7, 7, 7, 7, 7, 7, 7, 7
    };
    public final static short[] leadingRound = {0, 0, 0, 0, 0, 0, 0, 0,
            8, 8, 8, 8, 12, 12, 12, 12,
            16, 16, 18, 18, 20, 20, 22, 22,
            24, 24, 24, 24, 24, 24, 24, 24,
            24, 24, 24, 24, 24, 24, 24, 24,
            24, 24, 24, 24, 24, 24, 24, 24,
            24, 24, 24, 24, 24, 24, 24, 24,
            24, 24, 24, 24, 24, 24, 24, 24
    };
    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedVal = 0;
    private boolean first = true;
    private final OutputBitStream out;

    // We should have access to the series?
    public ChimpXORCompressor32() {
        int capacity = 1000;
        out = new OutputBitStream(
                new byte[(int) (((capacity + 1) * 4 + capacity / 4 + 1) * 1.2)]);
    }

    public OutputBitStream getOutputStream() {
        return this.out;
    }

    /**
     * Adds a new int value to the series. Note, values must be inserted in order.
     *
     * @param value next floating point value in the series
     */
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
        out.writeInt(storedVal, 32);
        return 32;
    }

    /**
     * Closes the block and writes the remaining stuff to the BitOutput.
     */
    @Override
    public int close() {
        int thisSize = addValue(Elf32Utils.END_SIGN);
        out.writeBit(false);
        out.flush();
        return thisSize;
    }

    private int compressValue(int value) {
        int thisSize = 0;
        int xor = storedVal ^ value;
        if (xor == 0) {
            // Write 0
            out.writeBit(false);
            out.writeBit(false);
            thisSize += 2;
            storedLeadingZeros = 65;
        } else {
            int leadingZeros = leadingRound[Integer.numberOfLeadingZeros(xor)];
            int trailingZeros = Integer.numberOfTrailingZeros(xor);

            if (trailingZeros > THRESHOLD) {
                int significantBits = 32 - leadingZeros - trailingZeros;
                out.writeBit(false);
                out.writeBit(true);
                out.writeInt(leadingRepresentation[leadingZeros], 3);
                out.writeInt(significantBits, 5);
                out.writeInt(xor >>> trailingZeros, significantBits); // Store the meaningful bits of XOR
                thisSize += 10 + significantBits;
                storedLeadingZeros = 33;
            } else if (leadingZeros == storedLeadingZeros) {
                out.writeBit(true);
                out.writeBit(false);
                int significantBits = 32 - leadingZeros;
                out.writeInt(xor, significantBits);
                thisSize += 2 + significantBits;
            } else {
                storedLeadingZeros = leadingZeros;
                int significantBits = 32 - leadingZeros;
                out.writeBit(true);
                out.writeBit(true);
                out.writeInt(leadingRepresentation[leadingZeros], 3);
                out.writeInt(xor, significantBits);
                thisSize += 5 + significantBits;
            }
        }
        storedVal = value;
        return thisSize;
    }

    public byte[] getOut() {
        return out.getBuffer();
    }

    @Override
    public void refresh() {
        out.refresh();
        storedLeadingZeros = Integer.MAX_VALUE;
        storedVal = 0;
        first = true;
    }
}
