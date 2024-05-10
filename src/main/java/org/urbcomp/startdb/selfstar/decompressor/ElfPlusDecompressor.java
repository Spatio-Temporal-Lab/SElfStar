package org.urbcomp.startdb.selfstar.decompressor;


import org.urbcomp.startdb.selfstar.decompressor.xor.IXORDecompressor;
import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.InputBitStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElfPlusDecompressor implements IDecompressor,INetDecompressor{
    private final IXORDecompressor xorDecompressor;
    private int lastBetaStar = Integer.MAX_VALUE;

    public ElfPlusDecompressor(IXORDecompressor xorDecompressor) {
        this.xorDecompressor = xorDecompressor;
    }

    public List<Double> decompress() {
        List<Double> values = new ArrayList<>(1024);
        Double value;
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
    public Double nextValue() {
        Double v;

        if (readInt(1) == 0) {
            v = recoverVByBetaStar();               // case 0
        } else if (readInt(1) == 0) {
            v = xorDecompressor.readValue();        // case 10
        } else {
            lastBetaStar = readInt(4);          // case 11
            v = recoverVByBetaStar();
        }
        return v;
    }

    private Double recoverVByBetaStar() {
        double v;
        Double vPrime = xorDecompressor.readValue();
        int sp = Elf64Utils.getSP(vPrime < 0 ? -vPrime : vPrime);
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

    /**
     * used for transmit test, which decompress db one by one
     * @param input bits for single db
     * @return db decompressed
     */
    @Override
    public double decompress(byte[] input) {
        setBytes(input);
        return nextValue();
    }


}
