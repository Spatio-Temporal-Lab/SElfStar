package transmit;

import com.github.Cwida.alp.ALPCompression;
import com.github.Cwida.alp.ALPDecompression;
import org.junit.jupiter.api.Test;
import org.urbcomp.startdb.selfstar.compressor.*;
import org.urbcomp.startdb.selfstar.compressor.xor.*;
import org.urbcomp.startdb.selfstar.decompressor.*;
import org.urbcomp.startdb.selfstar.decompressor.xor.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        for (int maxRate = 500; maxRate <= 1000; maxRate *= 2) {
            for (String fileName : fileNames) {
                INetCompressor[] compressors = {
                        // Put your compressors here
                        new SElfStarCompressor(new SElfXORCompressor()),
                        new SElfStarHuffmanCompressor(new SElfXORCompressor()),
//                        new ElfPlusCompressor(new ElfPlusXORCompressor()),
//                        new ElfCompressor(new ElfXORCompressor()),
//                        new BaseCompressor(new ChimpXORCompressor()),
//                        new BaseCompressor(new ChimpNXORCompressor(128)),
//                        new BaseCompressor(new GorillaXORCompressor()),
//                        new SBaseCompressor(new ChimpAdaXORCompressor()),
//                        new SBaseCompressor(new ChimpNAdaXORCompressor(128)),
                };
                INetDecompressor[] decompressors = {
                        // And put your corresponding decompressors heres
                        new ElfStarDecompressor(new SElfStarXORDecompressor()),
                        new SElfStarHuffmanDecompressor(new SElfStarXORDecompressor()),
//                        new ElfPlusDecompressor(new ElfPlusXORDecompressor()),
//                        new ElfDecompressor(new ElfXORDecompressor()),
//                        new BaseDecompressor(new ChimpXORDecompressor()),
//                        new BaseDecompressor(new ChimpNXORDecompressor(128)),
//                        new BaseDecompressor(new GorillaXORDecompressor()),
//                        new BaseDecompressor(new ChimpAdaXORDecompressor()),
//                        new BaseDecompressor(new ChimpNAdaXORDecompressor(128)),
                };

                for (int i = 0; i < compressors.length; i++) {
                    ReceiverThread receiverThread = new ReceiverThread(maxRate, decompressors[i], port);
//                    SenderThread senderThread = new SenderThread(maxRate, prefix + fileName, compressors[i], port);
                    SenderThread senderThread = new SenderThread(maxRate, fileName, compressors[i], port);
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
    public void ALPTest() throws InterruptedException {
        System.out.println("MaxRate,File,Method,Time");
        for (int maxRate = 500; maxRate <= 1000; maxRate *= 2) {
            for (String fileName : fileNames) {
                INetCompressor[] compressors = {
                        // Put your compressors here
                        new ALPCompression(100),
                };
                INetDecompressor[] decompressors = {
                        // And put your corresponding decompressors heres
                        new ALPDecompression(),
                };

                for (int i = 0; i < compressors.length; i++) {
                    ALPReceiverThread receiverThread = new ALPReceiverThread(maxRate, decompressors[i], port);
                    ALPSenderThread senderThread = new ALPSenderThread(maxRate, fileName, compressors[i], port);
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
    public void correctnessTest() throws IOException {
        // The same as above
//        for (double maxDiff : MAX_DIFF) {
        for (String fileName : fileNames) {
            INetCompressor[] compressors = {
//                    new SElfStarHuffmanCompressor(new SElfXORCompressor()),
                    new SElfStarCompressor(new SElfXORCompressor()),
//                    new ElfPlusCompressor(new ElfPlusXORCompressor()),
//                    new ElfCompressor(new ElfXORCompressor()),
//                    new BaseCompressor(new ChimpXORCompressor()),
//                    new BaseCompressor(new ChimpNXORCompressor(128)),
//                    new BaseCompressor(new GorillaXORCompressor()),
//                    new SBaseCompressor(new ChimpAdaXORCompressor()),
//                    new SBaseCompressor(new ChimpNAdaXORCompressor(128)),
            };
            INetDecompressor[] decompressors = {
//                    new SElfStarHuffmanDecompressor(new ElfStarXORDecompressor()),
                    new ElfStarDecompressor(new SElfStarXORDecompressor()),
//                    new ElfPlusDecompressor(new ElfPlusXORDecompressor()),
//                    new ElfDecompressor(new ElfXORDecompressor()),
//                    new BaseDecompressor(new ChimpXORDecompressor()),
//                    new BaseDecompressor(new ChimpNXORDecompressor(128)),
//                    new BaseDecompressor(new GorillaXORDecompressor()),
//                    new BaseDecompressor(new ChimpAdaXORDecompressor()),
//                    new BaseDecompressor(new ChimpNAdaXORDecompressor(128)),
            };
            for (int i = 0; i < compressors.length; i++) {
                Scanner scanner = new Scanner(new File(prefix + fileName));
                int cnt = 1;
                while (scanner.hasNextDouble()) {
                    if (cnt % 1001 == 0) {
//                        compressors[i].close();
                        compressors[i].refresh();
                        decompressors[i].refresh();
                    }
                    double value = scanner.nextDouble();
                    double deValue;
                    byte[] bytes;//= compressors[i].compress(value);
                    bytes = compressors[i].compress(value);
                    deValue = decompressors[i].decompress(Arrays.copyOfRange(bytes, 1, bytes.length));

//                    if (Math.abs(value - deValue) > maxDiff) {
                    if (value != deValue) {
                        System.out.println(compressors[i].getKey() + " " + fileName + " origin: " + value + " result: " + deValue);
                    }
                    assertTrue(Objects.equals(value, deValue)); // Infinity and NaN use
                    cnt++;
                }
            }
        }
//        }
        System.out.println("Done!");
    }

    @Test
    public void huffCorrectnessTest() throws IOException {
        // The same as above
//        for (double maxDiff : MAX_DIFF) {
        for (String fileName : fileNames) {
            INetCompressor[] compressors = {
                    new SElfStarHuffmanCompressor(new SElfXORCompressor()),
            };
            INetDecompressor[] decompressors = {
                    new SElfStarHuffmanDecompressor(new SElfStarXORDecompressor()),
            };
            for (int i = 0; i < compressors.length; i++) {
                Scanner scanner = new Scanner(new File(prefix + fileName));
                int cnt = 1;
                while (scanner.hasNextDouble()) {
                    double value = scanner.nextDouble();
                    double deValue;
                    byte[] bytes;
                    if (cnt % 1000 == 0 && cnt != 0) {
                        bytes = compressors[i].compressAndClose(value);
                        deValue = decompressors[i].decompressLast(Arrays.copyOfRange(bytes, 1, bytes.length));
                        compressors[i].refresh();
                        decompressors[i].refresh();

                    } else {
                        bytes = compressors[i].compress(value);
                        deValue = decompressors[i].decompress(Arrays.copyOfRange(bytes, 1, bytes.length));
                    }

                    if (value != deValue) {
                        System.out.println(compressors[i].getKey() + " " + fileName + " origin: " + value + " result: " + deValue);
                    }
                    assertTrue(Objects.equals(value, deValue)); // Infinity and NaN use
                    cnt++;
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
            int block = 1000;
            BlockReader br = new BlockReader(dataPath, block);
            List<Double> floatings;
            while ((floatings = br.nextBlock()) != null) {
                if (floatings.size() != block) {
                    break;
                }
                // one by one
                for (int i = 0; i < floatings.size() - 1; i++) {
                    double doubleToCompSend = floatings.get(i);
                    byte[] bytesToSend = compressor.compress(doubleToCompSend);
                    bytesToSend[0] = (byte) bytesToSend.length;
                    localClient.send(bytesToSend);
                }
                byte[] bytesToSend = compressor.compressAndClose(floatings.get(floatings.size() - 1));
                bytesToSend[0] = (byte) bytesToSend.length;
                localClient.send(bytesToSend);
                compressor.refresh();
            }
            // one by one
            localClient.send(new byte[]{(byte) 0});
            localClient.close();
        } catch (Exception e) {
            throw new RuntimeException(dataPath, e);
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
            int cnt = 0;
            while (true) {

                // one by one
                int byteCount = localServer.receiveBytes(1)[0];

                if (byteCount == 0) {
                    endTime = System.nanoTime();
                    break;
                }

                // one by one
                byte[] receivedData = localServer.receiveBytes(byteCount - 1);

                if (first) {
                    startTime = System.nanoTime();
                    first = false;
                }
                // one by one
                if ((cnt + 1) % 1000 == 0) {
                    decompressor.decompressLast(receivedData);
                    decompressor.refresh();
                } else {
                    decompressor.decompress(receivedData);
                }
                cnt++;

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