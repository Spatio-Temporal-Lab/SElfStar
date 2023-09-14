package org.urbcomp.startdb.selfstar.decompressor32;

import org.urbcomp.startdb.selfstar.decompressor32.xor.IXORDecompressor32;
import org.urbcomp.startdb.selfstar.utils.Elf32Utils;
import org.urbcomp.startdb.selfstar.utils.InputBitStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElfDecompressor32 implements IDecompressor32 {
    private final IXORDecompressor32 xorDecompressor;

    public ElfDecompressor32(IXORDecompressor32 xorDecompressor) {
        this.xorDecompressor = xorDecompressor;
    }

    @Override
    public List<Float> decompress() {
        List<Float> values = new ArrayList<>(1024);
        Float value;
        while ((value = nextValue()) != null) {
            values.add(value);
        }
        return values;
    }

    @Override
    public Float nextValue() {
        int flag = readInt(1);

        Float v;
        if (flag == 0) {
            v = xorDecompressor.readValue();
        } else {
            int betaStar = readInt(3);
            Float vPrime = xorDecompressor.readValue();
            int sp = (int) Math.floor(Math.log10(Math.abs(vPrime)));
            if (betaStar == 0) {
                v = Elf32Utils.get10iN(-sp - 1);
                if (vPrime < 0) {
                    v = -v;
                }
            } else {
                int alpha = betaStar - sp - 1;
                v = Elf32Utils.roundUp(vPrime, alpha);
            }
        }
        return v;
    }


    @Override
    public void setBytes(byte[] bs) {
        this.xorDecompressor.setBytes(bs);
    }

    @Override
    public void refresh() {
        xorDecompressor.refresh();
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
