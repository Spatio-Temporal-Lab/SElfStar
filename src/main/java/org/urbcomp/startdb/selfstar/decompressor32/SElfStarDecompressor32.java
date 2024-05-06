package org.urbcomp.startdb.selfstar.decompressor32;


import org.urbcomp.startdb.selfstar.decompressor32.xor.IXORDecompressor32;
import org.urbcomp.startdb.selfstar.utils.Elf32Utils;
import org.urbcomp.startdb.selfstar.utils.Huffman.Code;
import org.urbcomp.startdb.selfstar.utils.Huffman.HuffmanEncode;
import org.urbcomp.startdb.selfstar.utils.Huffman.Node;
import org.urbcomp.startdb.selfstar.utils.InputBitStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SElfStarDecompressor32 implements IDecompressor32 {

    private final IXORDecompressor32 xorDecompressor;
    private int lastBetaStar = Integer.MAX_VALUE;

    private final int[] frequency = new int[9];
    private boolean isFirst = true;
    private Node root;

    public SElfStarDecompressor32(IXORDecompressor32 xorDecompressor) {
        this.xorDecompressor = xorDecompressor;
    }

    public List<Float> decompress() {
        List<Float> values = new ArrayList<>(1024);
        Float value;
        while ((value = nextValue()) != null) {
            values.add(value);
        }
        frequency[8]--;
        Code[] huffmanCode = HuffmanEncode.getHuffmanCodes(frequency);
        root = HuffmanEncode.buildHuffmanTree(huffmanCode);
        Arrays.fill(frequency, 0);
        return values;
    }


    @Override
    public void refresh() {
        lastBetaStar = Integer.MAX_VALUE;
        xorDecompressor.refresh();
        isFirst = false;
    }

    @Override
    public void setBytes(byte[] bs) {
        this.xorDecompressor.setBytes(bs);
    }

    @Override
    public Float nextValue() {
        if (!isFirst) {
            return nextValueHuffman();
        } else {
            return nextValueFirst();
        }
    }


    public Float nextValueFirst() {
        Float v;
        if (readInt(1) == 0) {
            v = recoverVByBetaStar();               // case 0
            frequency[lastBetaStar]++;
        } else if (readInt(1) == 0) {
            v = xorDecompressor.readValue();        // case 10
            frequency[8]++;
        } else {
            lastBetaStar = readInt(3);          // case 11
            v = recoverVByBetaStar();
            frequency[lastBetaStar]++;
        }
        return v;
    }

    public Float nextValueHuffman() {
        Float v;
        Node current = root;
        while (true) {
            current = current.children[readInt(1)];
            if (current.data >= 0) {
                if (current.data != 8) {
                    lastBetaStar = current.data;
                    v = recoverVByBetaStar();
                    frequency[lastBetaStar]++;
                } else {
                    v = xorDecompressor.readValue();
                    frequency[8]++;
                }
                break;
            }
        }
        return v;
    }

    private Float recoverVByBetaStar() {
        float v;
        Float vPrime = xorDecompressor.readValue();
        int sp = Elf32Utils.getSP(Math.abs(vPrime));
        if (lastBetaStar == 0) {
            v = Elf32Utils.get10iN(-sp - 1);
            if (vPrime < 0) {
                v = -v;
            }
        } else {
            int alpha = lastBetaStar - sp - 1;
            v = Elf32Utils.roundUp(vPrime, alpha);
        }
        return v;
    }

    private int readInt(int len) {
        InputBitStream in = xorDecompressor.getInputStream();
        try {
            return in.readInt(len);
        } catch (IOException e) {
            throw new RuntimeException("IO error: " + e.getMessage());
        }
    }
}
