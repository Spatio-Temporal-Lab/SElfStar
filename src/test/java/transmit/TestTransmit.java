package transmit;

import org.junit.jupiter.api.Test;
import org.urbcomp.startdb.selfstar.compressor.INetCompressor;
import org.urbcomp.startdb.selfstar.decompressor.INetDecompressor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.Scanner;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTransmit {
    private static final String prefix = "src/main/resources/floating/";
    private static final double[] MAX_DIFF = new double[]{1.0E-1, 0.5, 1.0E-2, 1.0E-3, 1.0E-4, 1.0E-5, 1.0E-6, 1.0E-7, 1.0E-8};
    private static final double DEFAULT_DIFF = 0.0001;
    private static final int port = 10240;
    private final String[] fileNames = {
            "Air-pressure.csv",
            "Bird-migration.csv",
            "Bitcoin-price.csv",
            "Basel-temp.csv",
            "Basel-wind.csv",
            "City-temp.csv",
            "Dew-point-temp.csv",
            "IR-bio-temp.csv",
            "PM10-dust.csv",
            "Stocks-DE.csv",
            "Stocks-UK.csv",
            "Stocks-USA.csv",
            "Wind-Speed.csv"
    };

    @Test
    public void mainTest() throws InterruptedException {
        System.out.println("MaxRate,File,Method,Time");
        for (int maxRate = 1000; maxRate <= 10000; maxRate += 100) {
            for (String fileName : fileNames) {
                INetCompressor[] compressors = {
                        // Put your compressors here
                };
                INetDecompressor[] decompressors = {
                        // And put your corresponding decompressors heres
                };

                for (int i = 0; i < compressors.length; i++) {
                    ReceiverThread receiverThread = new ReceiverThread(maxRate, decompressors[i], port);
                    SenderThread senderThread = new SenderThread(maxRate, prefix + fileName, compressors[i], port);
                    receiverThread.start();
                    senderThread.start();
                    receiverThread.join();
                    System.out.println(maxRate + "," + fileName + "," + compressors[i].getKey() + "," + receiverThread.getUsedTimeInMS());
                    // Sleep for a second to wait system to close socket
                    Thread.sleep(1000);
                    System.gc();
                }
            }
        }
    }

    @Test
    public void correctnessTest() throws FileNotFoundException {
        // The same as above
        for (double maxDiff : MAX_DIFF) {
            for (String fileName : fileNames) {
                INetCompressor[] compressors = {

                };
                INetDecompressor[] decompressors = {

                };
                for (int i = 0; i < compressors.length; i++) {
                    Scanner scanner = new Scanner(new File(prefix + fileName));
                    while (scanner.hasNextDouble()) {
                        double value = scanner.nextDouble();
                        byte[] bytes = compressors[i].compress(value);
                        double deValue = decompressors[i].decompress(bytes);
                        if (Math.abs(value - deValue) > maxDiff) {
                            System.out.println(compressors[i].getKey() + " " + fileName + " " + maxDiff + " " + value + " " + deValue + " " + Math.abs(value - deValue));
                        }
                        assertTrue(Objects.equals(value, deValue) || // Infinity and NaN use
                                Math.abs(value - deValue) <= maxDiff);
                    }
                }
            }
        }
        System.out.println("Done!");
    }
}

class SenderThread extends Thread {
    private final int maxRate;
    private final String dataPath;
    private final INetCompressor compressor;
    private final int port;

    public SenderThread(int maxRate, String dataPath, INetCompressor compressor, int port) {
        super.setName("SenderThread");
        this.maxRate = maxRate;
        this.dataPath = dataPath;
        this.compressor = compressor;
        this.port = port;
    }

    @Override
    public void run() {
        super.run();
        try {
            Client localClient = new Client(maxRate, Optional.of(port));
            Scanner scanner = new Scanner(new File(dataPath));
            while (scanner.hasNextDouble()) {
                double doubleToCompSend = scanner.nextDouble();
                byte[] bytesToSend = compressor.compress(doubleToCompSend);
                bytesToSend[0] = (byte) (bytesToSend[0] | (((byte) bytesToSend.length) << 4));
                localClient.send(bytesToSend);
            }
            localClient.send(new byte[]{(byte) 0});
            localClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class ReceiverThread extends Thread {
    private final int maxRate;
    private final INetDecompressor decompressor;
    private final int port;
    private double usedTimeInMS;

    public ReceiverThread(int maxRate, INetDecompressor decompressor, int port) {
        super.setName("ReceiverThread");
        this.maxRate = maxRate;
        this.decompressor = decompressor;
        this.port = port;
    }

    @Override
    public void run() {
        super.run();
        try {
            boolean first = true;
            long startTime = 0;
            long endTime;
            Server localServer = new Server(maxRate, Optional.of(port));
            while (true) {
                byte header = localServer.receiveBytes(1)[0];
                int byteCount = header >>> 4;
                if (byteCount == 0) {
                    endTime = System.nanoTime();
                    break;
                }
                byte[] receivedData = localServer.receiveBytes(byteCount - 1);
                if (first) {
                    startTime = System.nanoTime();
                    first = false;
                }
                ByteBuffer buffer = ByteBuffer.allocate(byteCount + 1);
                buffer.put(header);
                buffer.put(receivedData);
                decompressor.decompress(buffer.array());
            }
            localServer.close();
            usedTimeInMS = (endTime - startTime) / 1000_000.0; // ms
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public double getUsedTimeInMS() {
        return usedTimeInMS;
    }
}
