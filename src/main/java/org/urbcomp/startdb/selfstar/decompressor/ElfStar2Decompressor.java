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

    private long[] lookUpArray;

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

    private static long[] generateLookupArray(List<Node> huffmanCodes, int maxCodeLength) {
        long[] lookupArray = new long[1 << maxCodeLength];
        Map<String, Long> codeMap = new HashMap<>();

        // 将霍夫曼编码映射为长整型码值
        for (HuffmanCode huffmanCode : huffmanCodes) {
            String binaryCode = Long.toBinaryString(huffmanCode.code);
            while (binaryCode.length() < huffmanCode.bitLength) {
                binaryCode = "0" + binaryCode;
            }
            codeMap.put(binaryCode, huffmanCode.code);
        }

        // 生成数组
        for (int i = 0; i < lookupArray.length; i++) {
            String binaryIndex = Integer.toBinaryString(i);
            while (binaryIndex.length() < maxCodeLength) {
                binaryIndex = "0" + binaryIndex;
            }

            for (Map.Entry<String, Long> entry : codeMap.entrySet()) {
                if (binaryIndex.startsWith(entry.getKey())) {
                    lookupArray[i] = entry.getValue();
                    break;
                }
            }
        }

        return lookupArray;
    }

    public void initHuffmanTree() {
        for (int state : states) {
            Node node = new Node(state);
            node.depth = readInt(3);
            huffmanCode.add(node);
            if (node.depth > maxCodeLen) {
                maxCodeLen = node.depth;
            }
        }
        CanonicalHuff.generateCode(huffmanCode);

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

        return 0.0;
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

    private int readBufferInt(int len, int bufferSize) {
        InputBitStream in = xorDecompressor.getInputStream();
        try {
            return in.readBufferInt(len, bufferSize);
        } catch (IOException e) {
            throw new RuntimeException("IO error: " + e.getMessage());
        }
    }
}
