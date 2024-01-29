package org.urbcomp.startdb.selfstar.mixdecompressor;

import javafx.util.Pair;
import org.urbcomp.startdb.selfstar.decompressor.IDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.xor.IXORDecompressor;
import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.Huffman.HuffmanEncode;
import org.urbcomp.startdb.selfstar.utils.Huffman.Node;
import org.urbcomp.startdb.selfstar.utils.InputBitStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ElfStarMixDecompressor implements IDecompressor {
    private static final HashMap<Integer, Pair<Long, Integer>> huffmanCode = new HashMap<>();
    private static final int[] states = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17};
    private final IXORDecompressor[] xorDecompressors;
    private IXORDecompressor xorDecompressor;
    private int lastBetaStar = Integer.MAX_VALUE;
    private Node root;

    public ElfStarMixDecompressor(IXORDecompressor[] xorDecompressors) {
        this.xorDecompressors = xorDecompressors;
    }

    public List<Double> decompress() {
        List<Double> values = new ArrayList<>(1024);
        readInt(1);

        initHuffmanTree();
        Double value;
        while ((value = nextValue()) != null) {
            values.add(value);
        }
        return values;
    }

    public void initHuffmanTree() {
        for (int state : states) {
            int length = readInt(5);
            long code = readInt(length);
            huffmanCode.put(state, new Pair<>(code, length));
        }
        root = HuffmanEncode.hashMapToTree(huffmanCode);
    }

    @Override
    public void refresh() {
        lastBetaStar = Integer.MAX_VALUE;
        xorDecompressor.refresh();
        huffmanCode.clear();
    }

    @Override
    public void setBytes(byte[] bs) {
        this.xorDecompressor.setBytes(bs);
    }

    @Override
    public Double nextValue() {
        Double v;
        Node current = root;
        while (true) {
            if (readInt(1) == 0) {
                current = current.left;
            } else {
                current = current.right;
            }
            if (current.data != -Integer.MAX_VALUE) {
                if (current.data != 17) {
                    lastBetaStar = current.data;
                    v = recoverVByBetaStar();
                } else {
                    v = xorDecompressor.readValue();
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
