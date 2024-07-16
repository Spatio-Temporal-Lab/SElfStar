package org.urbcomp.startdb.selfstar.decompressor32;

import org.urbcomp.startdb.selfstar.decompressor32.xor.IXORDecompressor32;
import org.urbcomp.startdb.selfstar.utils.Elf32Utils;
import org.urbcomp.startdb.selfstar.utils.Huffman.Code;
import org.urbcomp.startdb.selfstar.utils.Huffman.HuffmanEncode;
import org.urbcomp.startdb.selfstar.utils.Huffman.Node;
import org.urbcomp.startdb.selfstar.utils.InputBitStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElfStarDecompressor32 implements IDecompressor32 {
    private static Code[] huffmanCode = new Code[9];
    private final IXORDecompressor32 xorDecompressor;
    private int lastBetaStar = Integer.MAX_VALUE;
    private Node root;

    public ElfStarDecompressor32(IXORDecompressor32 xorDecompressor) {
        this.xorDecompressor = xorDecompressor;
    }

    public List<Float> decompress() {
        List<Float> values = new ArrayList<>(1024);
        initHuffmanTree();
        Float value;
        while ((value = nextValue()) != null) {
            values.add(value);
        }
        return values;
    }

    private void initHuffmanTree() {
        HuffmanEncode.readHuffmanCodes(xorDecompressor.getInputStream(), huffmanCode);
        root = HuffmanEncode.buildHuffmanTree(huffmanCode);
    }

    @Override
    public void refresh() {
        lastBetaStar = Integer.MAX_VALUE;
        xorDecompressor.refresh();
        huffmanCode = new Code[9];
    }

    @Override
    public void setBytes(byte[] bs) {
        this.xorDecompressor.setBytes(bs);
    }

    @Override
    public Float nextValue() {
        Float v;
        Node current = root;
        while (true) {
            current = current.children[readInt(1)];
            if (current.data >= 0) {
                if (current.data != 8) {
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

    private Float recoverVByBetaStar() {
        float v;
        Float vPrime = xorDecompressor.readValue();
        int sp = Elf32Utils.getSP(vPrime < 0 ? -vPrime : vPrime);
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

    @SuppressWarnings("all")
    private int readInt(int len) {
        InputBitStream in = xorDecompressor.getInputStream();
        try {
            return in.readInt(len);
        } catch (IOException e) {
            throw new RuntimeException("IO error: " + e.getMessage());
        }
    }
}
