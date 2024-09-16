package org.urbcomp.startdb.selfstar.decompressor32;

import org.urbcomp.startdb.selfstar.decompressor32.xor.IXORDecompressor32;
import org.urbcomp.startdb.selfstar.utils.Elf32Utils;
import org.urbcomp.startdb.selfstar.utils.InputBitStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElfPlusDecompressor32 implements IDecompressor32 {
    private final IXORDecompressor32 xorDecompressor;
    private int lastBetaStar = Integer.MAX_VALUE;

    public ElfPlusDecompressor32(IXORDecompressor32 xorDecompressor) {
        this.xorDecompressor = xorDecompressor;
    }

    public List<Float> decompress() {
        List<Float> values = new ArrayList<>(1024);
        Float value;
        while ((value = nextValue()) != null) {
            values.add(value);
        }
        return values;
    }

    @Override
    public void refresh() {
        lastBetaStar = Integer.MAX_VALUE;
        xorDecompressor.refresh();
    }

    @Override
    public void setBytes(byte[] bs) {
        this.xorDecompressor.setBytes(bs);
    }

    @Override
    public Float nextValue() {
        Float v;

        if (readInt(1) == 0) {
            v = recoverVByBetaStar();               // case 0
        } else if (readInt(1) == 0) {
            v = xorDecompressor.readValue();        // case 10
        } else {
            lastBetaStar = readInt(3);          // case 11
            v = recoverVByBetaStar();
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

    private int readInt(int len) {
        InputBitStream in = xorDecompressor.getInputStream();
        try {
            return in.readInt(len);
        } catch (IOException e) {
            throw new RuntimeException("IO error: " + e.getMessage());
        }
    }
}
