package org.urbcomp.startdb.selfstar.decompressor32;

import org.urbcomp.startdb.selfstar.decompressor32.xor.IXORDecompressor32;

import java.util.ArrayList;
import java.util.List;

public class BaseDecompressor32 implements IDecompressor32 {
    private final IXORDecompressor32 xorDecompressor;

    public BaseDecompressor32(IXORDecompressor32 xorDecompressor) {
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
        xorDecompressor.refresh();
    }

    @Override
    public void setBytes(byte[] bs) {
        this.xorDecompressor.setBytes(bs);
    }

    @Override
    public Float nextValue() {
        return xorDecompressor.readValue();
    }

}
