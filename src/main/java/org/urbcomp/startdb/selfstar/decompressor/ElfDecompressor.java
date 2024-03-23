package org.urbcomp.startdb.selfstar.decompressor;

import org.urbcomp.startdb.selfstar.decompressor.xor.IXORDecompressor;
import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.InputBitStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElfDecompressor implements IDecompressor, INetDecompressor {
    private final IXORDecompressor xorDecompressor;

    public ElfDecompressor(IXORDecompressor xorDecompressor) {
        this.xorDecompressor = xorDecompressor;
    }

    @Override
    public List<Double> decompress() {
        List<Double> values = new ArrayList<>(1024);
        Double value;
        while ((value = nextValue()) != null) {
            values.add(value);
        }
        return values;
    }

    @Override
    public Double nextValue() {
        int flag = readInt(1);

        Double v;
        if (flag == 0) {
            v = xorDecompressor.readValue();
        } else {
            int betaStar = readInt(4);
            Double vPrime = xorDecompressor.readValue();
            int sp = (int) Math.floor(Math.log10(Math.abs(vPrime)));
            if (betaStar == 0) {
                v = Elf64Utils.get10iN(-sp - 1);
                if (vPrime < 0) {
                    v = -v;
                }
            } else {
                int alpha = betaStar - sp - 1;
                v = Elf64Utils.roundUp(vPrime, alpha);
            }
        }
        return v;
    }


    @Override
    public void setBytes(byte[] bs) {
        this.xorDecompressor.setBytes(bs);
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
