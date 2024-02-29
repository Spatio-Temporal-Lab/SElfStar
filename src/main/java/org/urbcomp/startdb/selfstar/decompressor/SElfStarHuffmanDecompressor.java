package org.urbcomp.startdb.selfstar.decompressor;

import org.urbcomp.startdb.selfstar.decompressor.xor.IXORDecompressor;
import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.Huffman.Code;
import org.urbcomp.startdb.selfstar.utils.Huffman.HuffmanEncode;
import org.urbcomp.startdb.selfstar.utils.Huffman.Node;
import org.urbcomp.startdb.selfstar.utils.InputBitStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// all ElfStar (batch and stream) share the same decompressor
public class SElfStarHuffmanDecompressor implements IDecompressor {

    private final IXORDecompressor xorDecompressor;
    private int lastBetaStar = Integer.MAX_VALUE;

    private final int[] frequency = new int[18];
    private boolean isFirst = true;
    private Node root;

    public SElfStarHuffmanDecompressor(IXORDecompressor xorDecompressor) {
        this.xorDecompressor = xorDecompressor;
    }

    public List<Double> decompress() {
        List<Double> values = new ArrayList<>(1024);
        Double value;
        while ((value = nextValue()) != null) {
            values.add(value);
        }
        HuffmanEncode huffmanEncode = new HuffmanEncode(frequency);
        Code[] huffmanCode = huffmanEncode.getHuffmanCodes();
        root = HuffmanEncode.hashMapToTree(huffmanCode);
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
    public Double nextValue() {
        if (isFirst) {
            return nextValueFirst();
        } else {
            return nextValueHuffman();
        }
    }


    public Double nextValueFirst() {
        Double v;
        if (readInt(1) == 0) {
            v = recoverVByBetaStar();               // case 0
            frequency[lastBetaStar]++;
        } else if (readInt(1) == 0) {
            v = xorDecompressor.readValue();        // case 10
            frequency[17]++;
        } else {
            lastBetaStar = readInt(4);          // case 11
            v = recoverVByBetaStar();
            frequency[lastBetaStar]++;
        }
        return v;
    }

    public Double nextValueHuffman() {
        Double v;
        Node current = root;
        while (true) {
            current = current.children[readInt(1)];
            if (current.data != -Integer.MAX_VALUE) {
                if (current.data != 17) {
                    lastBetaStar = current.data;
                    v = recoverVByBetaStar();

                    frequency[lastBetaStar]++;
                } else {
                    v = xorDecompressor.readValue();
                    frequency[17]++;
                }
                break;
            }
        }
        return v;
    }

    private Double recoverVByBetaStar() {
        double v;
        Double vPrime = xorDecompressor.readValue();
        int sp = Elf64Utils.getSP(Math.abs(vPrime));
        if (lastBetaStar == 0) {
            v = Elf64Utils.get10iN(-sp - 1);
            if (vPrime < 0) {
                v = -v;
            }
        } else {
            int alpha = lastBetaStar - sp - 1;
            v = Elf64Utils.roundUp(vPrime, alpha);
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
