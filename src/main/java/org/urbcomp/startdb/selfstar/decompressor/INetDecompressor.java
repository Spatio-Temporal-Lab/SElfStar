package org.urbcomp.startdb.selfstar.decompressor;

public interface INetDecompressor {
    double decompress(byte[] input);

    void refresh();

    default double decompressLast(byte[] input){ return 0;};
}
