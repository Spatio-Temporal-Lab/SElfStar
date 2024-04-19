package org.urbcomp.startdb.selfstar.utils.Huffman;

public class Code {
    public final long value;
    public final int length;

    @Override
    public String toString() {
        return "Code{" +
                "value=" + value +
                ", length=" + length +
                '}';
    }

    public Code(long value, int length) {
        this.value = value;
        this.length = length;
    }
}
