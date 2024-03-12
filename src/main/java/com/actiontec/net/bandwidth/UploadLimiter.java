package com.actiontec.net.bandwidth;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Le
 */
public class UploadLimiter extends OutputStream {
    private final OutputStream os;
    private final BandwidthLimiter bandwidthLimiter;

    public UploadLimiter(OutputStream os, BandwidthLimiter bandwidthLimiter) {
        this.os = os;
        this.bandwidthLimiter = bandwidthLimiter;
    }

    @Override
    public void write(int b) throws IOException {
        if (bandwidthLimiter != null)
            bandwidthLimiter.limitNextBytes();
        this.os.write(b);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        if (bandwidthLimiter != null)
            bandwidthLimiter.limitNextBytes(len);
        this.os.write(b, off, len);
    }

}