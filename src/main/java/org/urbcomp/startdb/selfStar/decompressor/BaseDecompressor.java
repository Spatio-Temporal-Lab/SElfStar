package org.urbcomp.startdb.selfStar.decompressor;

import org.urbcomp.startdb.selfStar.decompressor.xor.IXORDecompressor;

import java.util.ArrayList;
import java.util.List;

public class BaseDecompressor implements IDecompressor {
    private final IXORDecompressor xorDecompressor;

    public BaseDecompressor(IXORDecompressor xorDecompressor) {
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
        xorDecompressor.refresh();
    }

    @Override
    public void setBytes(byte[] bs) {
        this.xorDecompressor.setBytes(bs);
    }

    @Override
    public Double nextValue() {
        return xorDecompressor.readValue();
    }

}
