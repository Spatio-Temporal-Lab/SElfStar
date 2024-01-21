package org.urbcomp.startdb.selfstar.decompressor;

import org.urbcomp.startdb.selfstar.decompressor.xor.IXORDecompressor;
import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.Huffman.CanonicalHuff;
import org.urbcomp.startdb.selfstar.utils.Huffman.Node;
import org.urbcomp.startdb.selfstar.utils.InputBitStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElfStar2Decompressor implements IDecompressor {
    private final List<Node> huffmanCode = new ArrayList<>();
    private static final int[] states = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17};
    private final IXORDecompressor xorDecompressor;
    private int lastBetaStar = Integer.MAX_VALUE;

    private int maxCodeLen = 0;

    private Node[] lookUpArray;

    public ElfStar2Decompressor(IXORDecompressor xorDecompressor) {
        this.xorDecompressor = xorDecompressor;
    }

    public List<Double> decompress() {
        List<Double> values = new ArrayList<>(1024);
        initHuffmanTree();
        Double value;
        while ((value = nextValue()) != null) {
            values.add(value);
        }
        return values;
    }


    //todo
    public static Node[] generateLookupArray(List<Node> huffmanCodes, int maxCodeLength) {
        Node[] lookupArray = new Node[1 << maxCodeLength];

        for (Node huffmanCode : huffmanCodes) {
            int failingBit = maxCodeLength - huffmanCode.depth;
            for (long i = huffmanCode.code << failingBit; i < (huffmanCode.code + 1) << failingBit; i++) {
                lookupArray[(int) i] = huffmanCode;
            }
        }
        return lookupArray;
    }

    public void initHuffmanTree() {
        for (int state : states) {
            Node node = new Node(state);
            node.depth = readInt(5);
            huffmanCode.add(node);
            if (node.depth > maxCodeLen) {
                maxCodeLen = node.depth;
            }
        }
        CanonicalHuff.generateCode(huffmanCode);
        System.out.println(huffmanCode);
        System.out.println(maxCodeLen);
        lookUpArray = generateLookupArray(huffmanCode, maxCodeLen);
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
        int lookupCode = readBufferInt(maxCodeLen);
        Node node = lookUpArray[lookupCode];
        setRemainBits(node.depth);
        System.out.println(node+" "+Integer.toBinaryString(lookupCode));
        if (node.data != 17) {
            lastBetaStar = node.data;
            v = recoverVByBetaStar();
        } else {
            v = xorDecompressor.readValue();
        }
        System.out.println(v);
        return v;
    }

    private Double recoverVByBetaStar() {
        double v;
        Double vPrime = xorDecompressor.readValue();
        System.out.println(vPrime);
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

    private int readBufferInt(int bufferSize) {
        InputBitStream in = xorDecompressor.getInputStream();
        return in.readIntToBuffer(bufferSize);

    }

    private void setRemainBits(int len) {
        InputBitStream in = xorDecompressor.getInputStream();
        in.setRemainBits(len);
    }
}
