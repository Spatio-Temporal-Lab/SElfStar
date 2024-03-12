package com.actiontec.net.bandwidth;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author Le
 */
public class DownloadLimiter extends InputStream {
    private final InputStream is;
    private final BandwidthLimiter bandwidthLimiter;

    public DownloadLimiter(InputStream is, BandwidthLimiter bandwidthLimiter) {
        this.is = is;
        this.bandwidthLimiter = bandwidthLimiter;
    }

    @Override
    public int read() throws IOException {
        if (this.bandwidthLimiter != null)
            this.bandwidthLimiter.limitNextBytes();
        return this.is.read();
    }

    public int read(byte[] b, int off, int len) throws IOException {
        if (bandwidthLimiter != null)
            bandwidthLimiter.limitNextBytes(len);
        return this.is.read(b, off, len);
    }
}