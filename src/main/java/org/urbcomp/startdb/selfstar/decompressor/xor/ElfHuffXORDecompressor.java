package org.urbcomp.startdb.selfstar.decompressor.xor;

import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.Huffman.Code;
import org.urbcomp.startdb.selfstar.utils.Huffman.HuffmanEncode;
import org.urbcomp.startdb.selfstar.utils.Huffman.Node;
import org.urbcomp.startdb.selfstar.utils.InputBitStream;

import java.io.IOException;
import java.util.Arrays;

public class ElfHuffXORDecompressor implements IXORDecompressor {
    private long storedVal = 0;
    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = Integer.MAX_VALUE;
    private boolean first = true;
    private boolean endOfStream = false;

    private InputBitStream in;

    private int[] leadingRepresentation = {0, 8, 12, 16, 18, 20, 22, 24};

    private int[] trailingRepresentation = {0, 22, 28, 32, 36, 40, 42, 46};

    private int leadingBitsPerValue = 3;

    private int trailingBitsPerValue = 3;

    private int[] leadingDistribution = new int[65];
    private int[] trailingDistribution = new int[64];

    private boolean isFirstBlock = true;
    private Node leadingRoot;
    private Node trailingRoot;

    public ElfHuffXORDecompressor() {
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
            if (isFirstBlock) {
                nextFirstBlock();
            } else {
                nextHuffman();
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
        if (endOfStream) {
            return null;
        }
        return Double.longBitsToDouble(storedVal);
    }

    private void nextFirstBlock() throws IOException {
        if (first) {
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

    private void nextHuffman() throws IOException {
        if (first) {
            Code[] leadingCode = HuffmanEncode.getHuffmanCodes(leadingDistribution);
            leadingRoot = HuffmanEncode.buildHuffmanTree(leadingCode);
            Code[] trailingCode = HuffmanEncode.getHuffmanCodes(trailingDistribution);
            trailingRoot = HuffmanEncode.buildHuffmanTree(trailingCode);
            Arrays.fill(leadingDistribution, 0);
            Arrays.fill(trailingDistribution, 0);

            first = false;
            int trailingZeros = in.readInt(7);
            if (trailingZeros < 64) {
                storedVal = ((in.readLong(63 - trailingZeros) << 1) + 1) << trailingZeros;
            } else {
                storedVal = 0;
            }
            endOfStream = storedVal == Elf64Utils.END_SIGN;

        } else {
            nextHuffmanValue();
        }
    }

    private void nextHuffmanValue() throws IOException {
        long value;
        int centerBits;
        Node leadingNodeCurrent = leadingRoot;
        Node trailingNodeCurrent = trailingRoot;
        while (true) {
            leadingNodeCurrent = leadingNodeCurrent.children[in.readInt(1)];
            if (leadingNodeCurrent.data >= 0) {
                storedLeadingZeros = leadingNodeCurrent.data;
                break;
            }
        }
        if (storedLeadingZeros < 64) {
            while (true) {
                trailingNodeCurrent = trailingNodeCurrent.children[in.readInt(1)];
                if (trailingNodeCurrent.data >= 0) {
                    storedTrailingZeros = trailingNodeCurrent.data;
                    break;
                }
            }
            centerBits = 64 - storedLeadingZeros - storedTrailingZeros;
            value = in.readLong(centerBits) << storedTrailingZeros;
            value = storedVal ^ value;
            endOfStream = value == Elf64Utils.END_SIGN;
            storedVal = value;
        }
    }

    private void nextValue() throws IOException {
        long value;
        int centerBits;

        if (in.readInt(1) == 1) {
            // case 1
            centerBits = 64 - storedLeadingZeros - storedTrailingZeros;
            value = in.readLong(centerBits) << storedTrailingZeros;
            leadingDistribution[Long.numberOfLeadingZeros(value)]++;
            trailingDistribution[Long.numberOfTrailingZeros(value)]++;
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
            leadingDistribution[Long.numberOfLeadingZeros(value)]++;
            trailingDistribution[Long.numberOfTrailingZeros(value)]++;
            value = storedVal ^ value;
            endOfStream = value == Elf64Utils.END_SIGN;
            storedVal = value;
        } else {
            leadingDistribution[64]++;
        }
    }
}
