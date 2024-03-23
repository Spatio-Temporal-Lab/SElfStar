package org.urbcomp.startdb.selfstar.decompressor;

import org.urbcomp.startdb.selfstar.decompressor.xor.IXORDecompressor;

import java.util.ArrayList;
import java.util.List;

public class BaseDecompressor implements IDecompressor, INetDecompressor {
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

    @Override
    public void setBytes(byte[] bs) {
        this.xorDecompressor.setBytes(bs);
    }

    @Override
    public Double nextValue() {
        return xorDecompressor.readValue();
    }

}
