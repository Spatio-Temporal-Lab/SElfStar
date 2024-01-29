import com.github.Cwida.alp.ALPCompression;
import com.github.Cwida.alp.ALPDecompression;
import com.github.Tranway.buff.BuffCompressor;
import com.github.Tranway.buff.BuffDecompressor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.compress.xerial.SnappyCodec;
import org.apache.hadoop.hbase.io.compress.xz.LzmaCodec;
import org.apache.hadoop.hbase.io.compress.zstd.ZstdCodec;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.junit.jupiter.api.Test;
import org.urbcomp.startdb.selfstar.compressor.*;
import org.urbcomp.startdb.selfstar.compressor.xor.*;
import org.urbcomp.startdb.selfstar.decompressor.*;
import org.urbcomp.startdb.selfstar.decompressor.xor.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCompressor {

    private static final String STORE_FILE = "src/test/resources/result/result2222.csv";
    private static final String STORE_PRUNING_FILE = "src/test/resources/result/resultPruning.csv";
    private static final String STORE_WINDOW_FILE = "src/test/resources/result/resultWindow.csv";
    private static final String STORE_BLOCK_FILE = "src/test/resources/result/resultBlock.csv";
    private static final double TIME_PRECISION = 1000.0;
    private static final int BLOCK_SIZE = 1000;
    private static final int NO_PARAM = 0;
    private static final String INIT_FILE = "init.csv";     // warm up memory and cpu
    private final String[] fileNames = {
            INIT_FILE,
            "Air-pressure.csv",
            "Air-sensor.csv",
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
            "Wind-Speed.csv",
            "Blockchain-tr.csv",
            "City-lat.csv",
            "City-lon.csv",
            "Food-price.csv",
            "POI-lat.csv",
            "POI-lon.csv",
            "SSD-bench.csv",
            "electric_vehicle_charging.csv"
    };

    private final Map<String, Long> fileNameParamToTotalBits = new HashMap<>();
    private final Map<String, Long> fileNameParamToTotalBlock = new HashMap<>();
    private final Map<String, Long> fileNameParamMethodToCompressedBits = new HashMap<>();
    private final Map<String, Double> fileNameParamMethodToCompressTime = new HashMap<>();
    private final Map<String, Double> fileNameParamMethodToDecompressTime = new HashMap<>();
    private final Map<String, Double> fileNameParamMethodToCompressedRatio = new TreeMap<>();// use TreeMap to keep the order
    private final int[] windowSizes = new int[]{100, 200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000, 3000, 4000};
    private final int[] blockSizes = new int[]{100, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000};

    private static double[] toDoubleArray(byte[] byteArray) {
        int times = Double.SIZE / Byte.SIZE;
        double[] doubles = new double[byteArray.length / times];
        for (int i = 0; i < doubles.length; i++) {
            doubles[i] = ByteBuffer.wrap(byteArray, i * times, times).getDouble();
        }
        return doubles;
    }

    @Test
    public void testAllCompressor() {
        for (String fileName : fileNames) {
            testALPCompressor(fileName, NO_PARAM);
//            testXZCompressor(fileName, NO_PARAM);
//            testZstdCompressor(fileName, NO_PARAM);
//            testSnappyCompressor(fileName, NO_PARAM);
            testBuffCompressor(fileName, NO_PARAM);
            testFloatingCompressor(fileName);
        }
        fileNameParamMethodToCompressedBits.forEach((fileNameParamMethod, compressedBits) -> {
            String fileNameParam = fileNameParamMethod.split(",")[0] + "," + fileNameParamMethod.split(",")[1];
            long fileTotalBits = fileNameParamToTotalBits.get(fileNameParam);
            fileNameParamMethodToCompressedRatio.put(fileNameParamMethod, (compressedBits * 1.0) / fileTotalBits);
        });
        System.out.println("Test All Compressor");
        writeResult(STORE_FILE, fileNameParamMethodToCompressedRatio, fileNameParamMethodToCompressTime, fileNameParamMethodToDecompressTime, fileNameParamToTotalBlock);
        System.gc();
    }

    //In this experiment, we implement window by block.
    @Test
    public void testPruningCompressor() {
        for (String fileName : fileNames) {
            testPruningFloatingCompressor(fileName);
        }
        fileNameParamMethodToCompressedBits.forEach((fileNameParamMethod, compressedBits) -> {
            String fileNameParam = fileNameParamMethod.split(",")[0] + "," + fileNameParamMethod.split(",")[1];
            long fileTotalBits = fileNameParamToTotalBits.get(fileNameParam);
            fileNameParamMethodToCompressedRatio.put(fileNameParamMethod, (compressedBits * 1.0) / fileTotalBits);
        });
        System.out.println("Test Pruning Compressor");
        writeResult(STORE_PRUNING_FILE, fileNameParamMethodToCompressedRatio, fileNameParamMethodToCompressTime, fileNameParamMethodToDecompressTime, fileNameParamToTotalBlock);
        System.gc();
    }

    //In this experiment, we implement window by block.
    @Test
    public void testWindowCompressor() {
        for (int window : windowSizes) {
            for (String fileName : fileNames) {
                ICompressor[] compressors = new ICompressor[]{
                        new ElfStarCompressor(new ElfStarXORCompressor(window), window),
                        new SElfStarCompressor(new SElfXORCompressor(window)),
                };

                IDecompressor[] decompressors = new IDecompressor[]{
                        new ElfStarDecompressor(new ElfStarXORDecompressor()),
                        new ElfStarDecompressor(new SElfStarXORDecompressor()),
                };
                testParamCompressor(fileName, window, compressors, decompressors);
            }
            fileNameParamMethodToCompressedBits.forEach((fileNameWindowMethod, compressedBits) -> {
                String fileNameWindow = fileNameWindowMethod.split(",")[0] + "," + fileNameWindowMethod.split(",")[1];
                long fileTotalBits = fileNameParamToTotalBits.get(fileNameWindow);
                fileNameParamMethodToCompressedRatio.put(fileNameWindowMethod, (compressedBits * 1.0) / fileTotalBits);
            });
            System.gc();
        }
        System.out.println("Test Window");
        writeResult(STORE_WINDOW_FILE, fileNameParamMethodToCompressedRatio, fileNameParamMethodToCompressTime, fileNameParamMethodToDecompressTime, fileNameParamToTotalBlock);
    }

    //This test takes about 10 minutes to complete.
    @Test
    public void testBlockCompressor() {
        for (int block : blockSizes) {
            for (String fileName : fileNames) {
                ICompressor[] compressors = new ICompressor[]{
                        new BaseCompressor(new ChimpXORCompressor(block)),
                        new BaseCompressor(new ChimpNXORCompressor(128, block)),
                        new BaseCompressor(new GorillaXORCompressor(block)),
                        new ElfCompressor(new ElfXORCompressor(block)),
                        new ElfStarCompressor(new ElfStarXORCompressor(block), block),
                };

                IDecompressor[] decompressors = new IDecompressor[]{
                        new BaseDecompressor(new ChimpXORDecompressor()),
                        new BaseDecompressor(new ChimpNXORDecompressor(128)),
                        new BaseDecompressor(new GorillaXORDecompressor()),
                        new ElfDecompressor(new ElfXORDecompressor()),
                        new ElfStarDecompressor(new ElfStarXORDecompressor()),
                };
                testALPCompressor(fileName, block);
                testBuffCompressor(fileName, block);
                testParamCompressor(fileName, block, compressors, decompressors);
                testZstdCompressor(fileName, block);
                testSnappyCompressor(fileName, block);
                testXZCompressor(fileName, block);
            }
            fileNameParamMethodToCompressedBits.forEach((fileNameBlockMethod, compressedBits) -> {
                String fileNameBlock = fileNameBlockMethod.split(",")[0] + "," + fileNameBlockMethod.split(",")[1];
                long fileTotalBits = fileNameParamToTotalBits.get(fileNameBlock);
                fileNameParamMethodToCompressedRatio.put(fileNameBlockMethod, (compressedBits * 1.0) / fileTotalBits);
            });
            System.gc();
        }
        System.out.println("Test Block");
        writeResult(STORE_BLOCK_FILE, fileNameParamMethodToCompressedRatio, fileNameParamMethodToCompressTime, fileNameParamMethodToDecompressTime, fileNameParamToTotalBlock);
    }

    private void testPruningFloatingCompressor(String fileName) {
        String fileNameParam = fileName + "," + NO_PARAM;
        fileNameParamToTotalBits.put(fileNameParam, 0L);
        fileNameParamToTotalBlock.put(fileNameParam, 0L);
        ICompressor[] compressors = new ICompressor[]{
                new ElfStarCompressor(new ElfStarXORCompressorNoFRZGPruning()),
                new ElfStarCompressor(new ElfStarXORCompressorNoFRPruning()),
                new ElfStarCompressor(new ElfStarXORCompressorNoFRZPruning()),
                new ElfStarCompressor(new ElfStarXORCompressor()),
        };

        IDecompressor[] decompressors = new IDecompressor[]{
                new ElfStarDecompressor(new ElfStarXORDecompressor()),
                new ElfStarDecompressor(new ElfStarXORDecompressor()),
                new ElfStarDecompressor(new ElfStarXORDecompressor()),
                new ElfStarDecompressor(new ElfStarXORDecompressor()),
        };
        boolean firstMethod = true;
        for (int i = 0; i < compressors.length; i++) {
            ICompressor compressor = compressors[i];
            try (BlockReader br = new BlockReader(fileName, BLOCK_SIZE)) {
                List<Double> floatings;

                while ((floatings = br.nextBlock()) != null) {

                    double compressTime = 0;
                    double decompressTime;
                    if (floatings.size() != BLOCK_SIZE) {
                        break;
                    }
                    if (firstMethod) {
                        fileNameParamToTotalBits.put(fileNameParam, fileNameParamToTotalBits.get(fileNameParam) + floatings.size() * 64L);
                        fileNameParamToTotalBlock.put(fileNameParam, fileNameParamToTotalBlock.get(fileNameParam) + 1L);
                    }
                    double start = System.nanoTime();
                    floatings.forEach(compressor::addValue);
                    compressor.close();
                    compressTime += (System.nanoTime() - start) / TIME_PRECISION;
                    IDecompressor decompressor = decompressors[i];
                    decompressor.setBytes(compressor.getBytes());

                    start = System.nanoTime();
                    List<Double> deValues = decompressor.decompress();
                    decompressTime = (System.nanoTime() - start) / TIME_PRECISION;

                    assertEquals(deValues.size(), floatings.size());
                    for (int j = 0; j < floatings.size(); j++) {
                        assertEquals(floatings.get(j), deValues.get(j));
                    }
                    String fileNameParamMethod = fileName + "," + NO_PARAM + "," + compressor.getKey();
                    if (!fileNameParamMethodToCompressedBits.containsKey(fileNameParamMethod)) {
                        fileNameParamMethodToCompressedBits.put(fileNameParamMethod, compressor.getCompressedSizeInBits());
                        fileNameParamMethodToCompressTime.put(fileNameParamMethod, compressTime);
                        fileNameParamMethodToDecompressTime.put(fileNameParamMethod, decompressTime);
                    } else {
                        long newSize = fileNameParamMethodToCompressedBits.get(fileNameParamMethod) + compressor.getCompressedSizeInBits();
                        double newCTime = fileNameParamMethodToCompressTime.get(fileNameParamMethod) + compressTime;
                        double newDTime = fileNameParamMethodToDecompressTime.get(fileNameParamMethod) + decompressTime;
                        fileNameParamMethodToCompressedBits.put(fileNameParamMethod, newSize);
                        fileNameParamMethodToCompressTime.put(fileNameParamMethod, newCTime);
                        fileNameParamMethodToDecompressTime.put(fileNameParamMethod, newDTime);
                    }
                    compressor.refresh();
                    decompressor.refresh();

                }
            } catch (Exception e) {
                throw new RuntimeException(fileName, e);
            }
            firstMethod = false;
        }
    }

    private void testFloatingCompressor(String fileName) {
        String fileNameParam = fileName + "," + NO_PARAM;
        fileNameParamToTotalBits.put(fileNameParam, 0L);
        fileNameParamToTotalBlock.put(fileNameParam, 0L);
        ICompressor[] compressors = new ICompressor[]{
                new BaseCompressor(new ChimpXORCompressor()),
                new BaseCompressor(new ChimpNXORCompressor(128)),
                new BaseCompressor(new GorillaXORCompressor()),
                new ElfCompressor(new ElfXORCompressor()),
                new ElfPlusCompressor(new ElfPlusXORCompressor()),
                new ElfStarCompressor(new ElfStarXORCompressorAdaLead()),
                new ElfStarCompressor(new ElfStarXORCompressorAdaLeadAdaTrail()),
                new ElfStarCompressor(new ElfStarXORCompressor()),
                new SElfStarCompressor(new SElfXORCompressor()),
                new ElfStarHuffmanCompressor(new ElfStarXORCompressor()),
        };

        IDecompressor[] decompressors = new IDecompressor[]{
                new BaseDecompressor(new ChimpXORDecompressor()),
                new BaseDecompressor(new ChimpNXORDecompressor(128)),
                new BaseDecompressor(new GorillaXORDecompressor()),
                new ElfDecompressor(new ElfXORDecompressor()),
                new ElfPlusDecompressor(new ElfPlusXORDecompressor()),
                new ElfStarDecompressor(new ElfStarXORDecompressorAdaLead()),
                new ElfStarDecompressor(new ElfStarXORDecompressorAdaLeadAdaTrail()),
                new ElfStarDecompressor(new ElfStarXORDecompressor()),
                new ElfStarDecompressor(new SElfStarXORDecompressor()),
                new ElfStarHuffmanDecompressor(new ElfStarXORDecompressor())
        };
        boolean firstMethod = true;
        for (int i = 0; i < compressors.length; i++) {
            ICompressor compressor = compressors[i];
            try (BlockReader br = new BlockReader(fileName, BLOCK_SIZE)) {
                List<Double> floatings;

                while ((floatings = br.nextBlock()) != null) {

                    double compressTime = 0;
                    double decompressTime;
                    if (floatings.size() != BLOCK_SIZE) {
                        break;
                    }
                    if (firstMethod) {
                        fileNameParamToTotalBits.put(fileNameParam, fileNameParamToTotalBits.get(fileNameParam) + floatings.size() * 64L);
                        fileNameParamToTotalBlock.put(fileNameParam, fileNameParamToTotalBlock.get(fileNameParam) + 1L);
                    }
                    double start = System.nanoTime();
                    floatings.forEach(compressor::addValue);
                    compressor.close();
                    compressTime += (System.nanoTime() - start) / TIME_PRECISION;
                    IDecompressor decompressor = decompressors[i];
                    decompressor.setBytes(compressor.getBytes());

                    start = System.nanoTime();
                    List<Double> deValues = decompressor.decompress();
                    decompressTime = (System.nanoTime() - start) / TIME_PRECISION;

                    assertEquals(deValues.size(), floatings.size());
                    for (int j = 0; j < floatings.size(); j++) {
                        assertEquals(floatings.get(j), deValues.get(j));
                    }
                    String fileNameParamMethod = fileName + "," + NO_PARAM + "," + compressor.getKey();
                    if (!fileNameParamMethodToCompressedBits.containsKey(fileNameParamMethod)) {
                        fileNameParamMethodToCompressedBits.put(fileNameParamMethod, compressor.getCompressedSizeInBits());
                        fileNameParamMethodToCompressTime.put(fileNameParamMethod, compressTime);
                        fileNameParamMethodToDecompressTime.put(fileNameParamMethod, decompressTime);
                    } else {
                        long newSize = fileNameParamMethodToCompressedBits.get(fileNameParamMethod) + compressor.getCompressedSizeInBits();
                        double newCTime = fileNameParamMethodToCompressTime.get(fileNameParamMethod) + compressTime;
                        double newDTime = fileNameParamMethodToDecompressTime.get(fileNameParamMethod) + decompressTime;
                        fileNameParamMethodToCompressedBits.put(fileNameParamMethod, newSize);
                        fileNameParamMethodToCompressTime.put(fileNameParamMethod, newCTime);
                        fileNameParamMethodToDecompressTime.put(fileNameParamMethod, newDTime);
                    }
                    compressor.refresh();
                    decompressor.refresh();
                }
            } catch (Exception e) {
                throw new RuntimeException(fileName, e);
            }
            firstMethod = false;
        }
    }

    private void testParamCompressor(String fileName, int window, ICompressor[] compressors, IDecompressor[] decompressors) {
        boolean firstMethod = true;
        String fileNameWindow = fileName + "," + window;
        fileNameParamToTotalBits.put(fileNameWindow, 0L);
        fileNameParamToTotalBlock.put(fileNameWindow, 0L);
        for (int i = 0; i < compressors.length; i++) {
            ICompressor compressor = compressors[i];
            try (BlockReader br = new BlockReader(fileName, window)) {
                List<Double> floatings;

                while ((floatings = br.nextBlock()) != null) {
                    double compressTime = 0;
                    double decompressTime;
                    if (floatings.size() != window) {
                        break;
                    }
                    if (firstMethod) {
                        fileNameParamToTotalBits.put(fileNameWindow, fileNameParamToTotalBits.get(fileNameWindow) + floatings.size() * 64L);
                        fileNameParamToTotalBlock.put(fileNameWindow, fileNameParamToTotalBlock.get(fileNameWindow) + 1L);
                    }
                    double start = System.nanoTime();
                    floatings.forEach(compressor::addValue);

                    compressor.close();
                    compressTime += (System.nanoTime() - start) / TIME_PRECISION;
                    IDecompressor decompressor = decompressors[i];
                    decompressor.setBytes(compressor.getBytes());

                    start = System.nanoTime();
                    List<Double> deValues = decompressor.decompress();
                    decompressTime = (System.nanoTime() - start) / TIME_PRECISION;

                    assertEquals(deValues.size(), floatings.size());
                    for (int j = 0; j < floatings.size(); j++) {
                        assertEquals(floatings.get(j), deValues.get(j));
                    }
                    String fileNameWindowMethod = fileName + "," + window + "," + compressor.getKey();
                    if (!fileNameParamMethodToCompressedBits.containsKey(fileNameWindowMethod)) {
                        fileNameParamMethodToCompressedBits.put(fileNameWindowMethod, compressor.getCompressedSizeInBits());
                        fileNameParamMethodToCompressTime.put(fileNameWindowMethod, compressTime * BLOCK_SIZE / window);
                        fileNameParamMethodToDecompressTime.put(fileNameWindowMethod, decompressTime * BLOCK_SIZE / window);
                    } else {
                        long newSize = fileNameParamMethodToCompressedBits.get(fileNameWindowMethod) + compressor.getCompressedSizeInBits();
                        double newCTime = fileNameParamMethodToCompressTime.get(fileNameWindowMethod) + compressTime * BLOCK_SIZE / window;
                        double newDTime = fileNameParamMethodToDecompressTime.get(fileNameWindowMethod) + decompressTime * BLOCK_SIZE / window;
                        fileNameParamMethodToCompressedBits.put(fileNameWindowMethod, newSize);
                        fileNameParamMethodToCompressTime.put(fileNameWindowMethod, newCTime);
                        fileNameParamMethodToDecompressTime.put(fileNameWindowMethod, newDTime);
                    }
                    compressor.refresh();
                    decompressor.refresh();
                }
            } catch (Exception e) {
                throw new RuntimeException(fileName, e);
            }
            firstMethod = false;
        }
    }

    private void testALPCompressor(String fileName, int block) {
        long compressorBits;
        String fileNameParam = fileName + "," + block;
        if (block == NO_PARAM) {
            block = BLOCK_SIZE;
        }
        fileNameParamToTotalBits.put(fileNameParam, 0L);
        fileNameParamToTotalBlock.put(fileNameParam, 0L);
        double encodingDuration = 0;
        double decodingDuration = 0;
        try (BlockReader br = new BlockReader(fileName, block)) {
            List<List<List<Double>>> RowGroups = new ArrayList<>();
            List<List<Double>> floatingsList = new ArrayList<>();
            List<Double> floatings;
            int RGsize = 100;
            while ((floatings = br.nextBlock()) != null) {
                if (floatings.size() != block) {
                    break;
                }
                floatingsList.add(new ArrayList<>(floatings));
                fileNameParamToTotalBits.put(fileNameParam, fileNameParamToTotalBits.get(fileNameParam) + floatings.size() * 64L);
                if (floatingsList.size() == RGsize) {
                    RowGroups.add(new ArrayList<>(floatingsList));
                    floatingsList.clear();
                }
                fileNameParamToTotalBlock.put(fileNameParam, fileNameParamToTotalBlock.get(fileNameParam) + 1L);
            }
            if (!floatingsList.isEmpty()) {
                RowGroups.add(floatingsList);
            }

            long start = System.nanoTime();
            ALPCompression compressor = new ALPCompression(block);
            for (List<List<Double>> rowGroup : RowGroups) {
                compressor.entry(rowGroup);
                compressor.reset();
            }
            compressor.flush();
            encodingDuration += System.nanoTime() - start;

            byte[] result = compressor.getOut();

            start = System.nanoTime();
            ALPDecompression decompressor = new ALPDecompression(result);

            List<List<double[]>> deValues = new ArrayList<>();
            for (int i = 0; i < RowGroups.size(); i++) {
                List<double[]> deValue = decompressor.entry();
                deValues.add(deValue);
            }
            decodingDuration += System.nanoTime() - start;

            for (int RGidx = 0; RGidx < RowGroups.size(); RGidx++) {
                for (int i = 0; i < RowGroups.get(RGidx).size(); i++) {
                    for (int j = 0; j < RowGroups.get(RGidx).get(i).size(); j++) {
                        assertEquals(RowGroups.get(RGidx).get(i).get(j), deValues.get(RGidx).get(i)[j], "Value did not match");
                    }
                }
            }
            compressorBits = compressor.getSize();
            String fileNameParamMethod = fileNameParam + "," + "ALP";
            if (!fileNameParamMethodToCompressedBits.containsKey(fileNameParamMethod)) {
                fileNameParamMethodToCompressedBits.put(fileNameParamMethod, compressorBits);
                fileNameParamMethodToCompressTime.put(fileNameParamMethod, encodingDuration / TIME_PRECISION * BLOCK_SIZE / block);
                fileNameParamMethodToDecompressTime.put(fileNameParamMethod, decodingDuration / TIME_PRECISION * BLOCK_SIZE / block);
            } else {
                long newSize = fileNameParamMethodToCompressedBits.get(fileNameParamMethod) + compressorBits;
                double newCTime = fileNameParamMethodToCompressTime.get(fileNameParamMethod) + encodingDuration / TIME_PRECISION * BLOCK_SIZE / block;
                double newDTime = fileNameParamMethodToDecompressTime.get(fileNameParamMethod) + decodingDuration / TIME_PRECISION * BLOCK_SIZE / block;
                fileNameParamMethodToCompressedBits.put(fileNameParamMethod, newSize);
                fileNameParamMethodToCompressTime.put(fileNameParamMethod, newCTime);
                fileNameParamMethodToDecompressTime.put(fileNameParamMethod, newDTime);
            }

        } catch (Exception e) {
            throw new RuntimeException(fileName, e);
        }
    }

    private void testBuffCompressor(String fileName, int block) {
        long compressorBits;
        String fileNameParam = fileName + "," + block;
        if (block == NO_PARAM) {
            block = BLOCK_SIZE;
        }
        fileNameParamToTotalBits.put(fileNameParam, 0L);
        fileNameParamToTotalBlock.put(fileNameParam, 0L);
        try (BlockReader br = new BlockReader(fileName, block)) {
            List<Double> floatings;
            while ((floatings = br.nextBlock()) != null) {

                if (floatings.size() != block) {
                    break;
                }
                double[] values = floatings.stream()
                        .mapToDouble(Double::doubleValue)
                        .toArray();
                fileNameParamToTotalBits.put(fileNameParam, fileNameParamToTotalBits.get(fileNameParam) + floatings.size() * 64L);
                fileNameParamToTotalBlock.put(fileNameParam, fileNameParamToTotalBlock.get(fileNameParam) + 1L);
                double encodingDuration = 0;
                double decodingDuration = 0;
                BuffCompressor compressor = new BuffCompressor(block);
                // Compress
                long start = System.nanoTime();
                compressor.compress(values);
                encodingDuration += System.nanoTime() - start;
                compressorBits = compressor.getSize();

                byte[] result = compressor.getOut();
                BuffDecompressor decompressor = new BuffDecompressor(result);

                start = System.nanoTime();

                double[] uncompressed = decompressor.decompress();
                decodingDuration += System.nanoTime() - start;

                // Decompressed bytes should equal the original
                for (int i = 0; i < floatings.size(); i++) {
                    assertEquals(floatings.get(i), uncompressed[i], "Value did not match");
                }
                String fileNameParamMethod = fileNameParam + "," + "Buff";
                if (!fileNameParamMethodToCompressedBits.containsKey(fileNameParamMethod)) {
                    fileNameParamMethodToCompressedBits.put(fileNameParamMethod, compressorBits);
                    fileNameParamMethodToCompressTime.put(fileNameParamMethod, encodingDuration / TIME_PRECISION * BLOCK_SIZE / block);
                    fileNameParamMethodToDecompressTime.put(fileNameParamMethod, decodingDuration / TIME_PRECISION * BLOCK_SIZE / block);
                } else {
                    long newSize = fileNameParamMethodToCompressedBits.get(fileNameParamMethod) + compressorBits;
                    double newCTime = fileNameParamMethodToCompressTime.get(fileNameParamMethod) + encodingDuration / TIME_PRECISION * BLOCK_SIZE / block;
                    double newDTime = fileNameParamMethodToDecompressTime.get(fileNameParamMethod) + decodingDuration / TIME_PRECISION * BLOCK_SIZE / block;
                    fileNameParamMethodToCompressedBits.put(fileNameParamMethod, newSize);
                    fileNameParamMethodToCompressTime.put(fileNameParamMethod, newCTime);
                    fileNameParamMethodToDecompressTime.put(fileNameParamMethod, newDTime);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(fileName, e);
        }
    }

    private void testXZCompressor(String fileName, int block) {
        long compressorBits;
        String fileNameParam = fileName + "," + block;
        if (block == NO_PARAM) {
            block = BLOCK_SIZE;
        }
        fileNameParamToTotalBits.put(fileNameParam, 0L);
        fileNameParamToTotalBlock.put(fileNameParam, 0L);
        try (BlockReader br = new BlockReader(fileName, block)) {
            List<Double> floatings;
            while ((floatings = br.nextBlock()) != null) {

                if (floatings.size() != block) {
                    break;
                }
                fileNameParamToTotalBits.put(fileNameParam, fileNameParamToTotalBits.get(fileNameParam) + floatings.size() * 64L);
                fileNameParamToTotalBlock.put(fileNameParam, fileNameParamToTotalBlock.get(fileNameParam) + 1L);
                double encodingDuration = 0;
                double decodingDuration = 0;

                Configuration conf = new Configuration();
                // LZMA levels range from 1 to 9.
                // Level 9 might take several minutes to complete. 3 is our default. 1 will be fast.
                conf.setInt(LzmaCodec.LZMA_LEVEL_KEY, 3);
                LzmaCodec codec = new LzmaCodec();
                codec.setConf(conf);

                // Compress
                long start = System.nanoTime();
                org.apache.hadoop.io.compress.Compressor compressor = codec.createCompressor();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                CompressionOutputStream out = codec.createOutputStream(baos, compressor);
                ByteBuffer bb = ByteBuffer.allocate(floatings.size() * 8);
                for (double d : floatings) {
                    bb.putDouble(d);
                }
                byte[] input = bb.array();
                out.write(input);
                out.close();
                encodingDuration += System.nanoTime() - start;

                final byte[] compressed = baos.toByteArray();
                compressorBits = compressed.length * 8L;

                final byte[] plain = new byte[input.length];
                org.apache.hadoop.io.compress.Decompressor decompressor = codec.createDecompressor();
                start = System.nanoTime();
                CompressionInputStream in = codec.createInputStream(new ByteArrayInputStream(compressed), decompressor);
                IOUtils.readFully(in, plain, 0, plain.length);
                in.close();
                double[] uncompressed = toDoubleArray(plain);
                decodingDuration += System.nanoTime() - start;

                // Decompressed bytes should equal the original
                for (int i = 0; i < floatings.size(); i++) {
                    assertEquals(floatings.get(i), uncompressed[i], "Value did not match");
                }
                String fileNameParamMethod = fileNameParam + "," + "Xz";
                if (!fileNameParamMethodToCompressedBits.containsKey(fileNameParamMethod)) {
                    fileNameParamMethodToCompressedBits.put(fileNameParamMethod, compressorBits);
                    fileNameParamMethodToCompressTime.put(fileNameParamMethod, encodingDuration / TIME_PRECISION * BLOCK_SIZE / block);
                    fileNameParamMethodToDecompressTime.put(fileNameParamMethod, decodingDuration / TIME_PRECISION * BLOCK_SIZE / block);
                } else {
                    long newSize = fileNameParamMethodToCompressedBits.get(fileNameParamMethod) + compressorBits;
                    double newCTime = fileNameParamMethodToCompressTime.get(fileNameParamMethod) + encodingDuration / TIME_PRECISION * BLOCK_SIZE / block;
                    double newDTime = fileNameParamMethodToDecompressTime.get(fileNameParamMethod) + decodingDuration / TIME_PRECISION * BLOCK_SIZE / block;
                    fileNameParamMethodToCompressedBits.put(fileNameParamMethod, newSize);
                    fileNameParamMethodToCompressTime.put(fileNameParamMethod, newCTime);
                    fileNameParamMethodToDecompressTime.put(fileNameParamMethod, newDTime);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(fileName, e);
        }
    }

    private void testZstdCompressor(String fileName, int block) {
        long compressorBits;
        String fileNameParam = fileName + "," + block;
        if (block == NO_PARAM) {
            block = BLOCK_SIZE;
        }
        fileNameParamToTotalBits.put(fileNameParam, 0L);
        fileNameParamToTotalBlock.put(fileNameParam, 0L);
        try (BlockReader br = new BlockReader(fileName, block)) {
            List<Double> floatings;
            while ((floatings = br.nextBlock()) != null) {

                if (floatings.size() != block) {
                    break;
                }

                fileNameParamToTotalBits.put(fileNameParam, fileNameParamToTotalBits.get(fileNameParam) + floatings.size() * 64L);
                fileNameParamToTotalBlock.put(fileNameParam, fileNameParamToTotalBlock.get(fileNameParam) + 1L);
                double encodingDuration = 0;
                double decodingDuration = 0;

                Configuration conf = HBaseConfiguration.create();
                // LZMA levels range from 1 to 9.
                // Level 9 might take several minutes to complete. 3 is our default. 1 will be fast.
                conf.setInt(CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_LEVEL_KEY, 3);
                ZstdCodec codec = new ZstdCodec();
                codec.setConf(conf);

                // Compress
                long start = System.nanoTime();
                org.apache.hadoop.io.compress.Compressor compressor = codec.createCompressor();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                CompressionOutputStream out = codec.createOutputStream(baos, compressor);
                ByteBuffer bb = ByteBuffer.allocate(floatings.size() * 8);
                for (double d : floatings) {
                    bb.putDouble(d);
                }
                byte[] input = bb.array();
                out.write(input);
                out.close();
                encodingDuration += System.nanoTime() - start;

                final byte[] compressed = baos.toByteArray();
                compressorBits = compressed.length * 8L;

                final byte[] plain = new byte[input.length];
                org.apache.hadoop.io.compress.Decompressor decompressor = codec.createDecompressor();
                start = System.nanoTime();
                CompressionInputStream in = codec.createInputStream(new ByteArrayInputStream(compressed), decompressor);
                IOUtils.readFully(in, plain, 0, plain.length);
                in.close();
                double[] uncompressed = toDoubleArray(plain);
                decodingDuration += System.nanoTime() - start;
                // Decompressed bytes should equal the original
                for (int i = 0; i < floatings.size(); i++) {
                    assertEquals(floatings.get(i), uncompressed[i], "Value did not match");
                }
                String fileNameMethod = fileNameParam + "," + "Zstd";
                if (!fileNameParamMethodToCompressedBits.containsKey(fileNameMethod)) {
                    fileNameParamMethodToCompressedBits.put(fileNameMethod, compressorBits);
                    fileNameParamMethodToCompressTime.put(fileNameMethod, encodingDuration / TIME_PRECISION * BLOCK_SIZE / block);
                    fileNameParamMethodToDecompressTime.put(fileNameMethod, decodingDuration / TIME_PRECISION * BLOCK_SIZE / block);
                } else {
                    long newSize = fileNameParamMethodToCompressedBits.get(fileNameMethod) + compressorBits;
                    double newCTime = fileNameParamMethodToCompressTime.get(fileNameMethod) + encodingDuration / TIME_PRECISION * BLOCK_SIZE / block;
                    double newDTime = fileNameParamMethodToDecompressTime.get(fileNameMethod) + decodingDuration / TIME_PRECISION * BLOCK_SIZE / block;
                    fileNameParamMethodToCompressedBits.put(fileNameMethod, newSize);
                    fileNameParamMethodToCompressTime.put(fileNameMethod, newCTime);
                    fileNameParamMethodToDecompressTime.put(fileNameMethod, newDTime);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(fileName, e);
        }
    }

    private void testSnappyCompressor(String fileName, int block) {
        long compressorBits;
        String fileNameParam = fileName + "," + block;
        if (block == NO_PARAM) {
            block = BLOCK_SIZE;
        }
        fileNameParamToTotalBits.put(fileNameParam, 0L);
        fileNameParamToTotalBlock.put(fileNameParam, 0L);
        try (BlockReader br = new BlockReader(fileName, block)) {
            List<Double> floatings;
            while ((floatings = br.nextBlock()) != null) {
                if (floatings.size() != block) {
                    break;
                }

                fileNameParamToTotalBits.put(fileNameParam, fileNameParamToTotalBits.get(fileNameParam) + floatings.size() * 64L);
                fileNameParamToTotalBlock.put(fileNameParam, fileNameParamToTotalBlock.get(fileNameParam) + 1L);
                double encodingDuration = 0;
                double decodingDuration = 0;

                Configuration conf = HBaseConfiguration.create();
                // LZMA levels range from 1 to 9.
                // Level 9 might take several minutes to complete. 3 is our default. 1 will be fast.
                conf.setInt(CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_LEVEL_KEY, 3);
                SnappyCodec codec = new SnappyCodec();
                codec.setConf(conf);

                // Compress
                long start = System.nanoTime();
                org.apache.hadoop.io.compress.Compressor compressor = codec.createCompressor();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                CompressionOutputStream out = codec.createOutputStream(baos, compressor);
                ByteBuffer bb = ByteBuffer.allocate(floatings.size() * 8);
                for (double d : floatings) {
                    bb.putDouble(d);
                }
                byte[] input = bb.array();
                out.write(input);
                out.close();
                encodingDuration += System.nanoTime() - start;
                final byte[] compressed = baos.toByteArray();
                compressorBits = compressed.length * 8L;

                final byte[] plain = new byte[input.length];
                org.apache.hadoop.io.compress.Decompressor decompressor = codec.createDecompressor();
                start = System.nanoTime();
                CompressionInputStream in = codec.createInputStream(new ByteArrayInputStream(compressed), decompressor);
                IOUtils.readFully(in, plain, 0, plain.length);
                in.close();
                double[] uncompressed = toDoubleArray(plain);
                decodingDuration += System.nanoTime() - start;

                // Decompressed bytes should equal the original
                for (int i = 0; i < floatings.size(); i++) {
                    assertEquals(floatings.get(i), uncompressed[i], "Value did not match");
                }

                System.gc();
                String fileNameParamMethod = fileNameParam + "," + "Snappy";
                if (!fileNameParamMethodToCompressedBits.containsKey(fileNameParamMethod)) {
                    fileNameParamMethodToCompressedBits.put(fileNameParamMethod, compressorBits);
                    fileNameParamMethodToCompressTime.put(fileNameParamMethod, encodingDuration / TIME_PRECISION * BLOCK_SIZE / block);
                    fileNameParamMethodToDecompressTime.put(fileNameParamMethod, decodingDuration / TIME_PRECISION * BLOCK_SIZE / block);
                } else {
                    long newSize = fileNameParamMethodToCompressedBits.get(fileNameParamMethod) + compressorBits;
                    double newCTime = fileNameParamMethodToCompressTime.get(fileNameParamMethod) + encodingDuration / TIME_PRECISION * BLOCK_SIZE / block;
                    double newDTime = fileNameParamMethodToDecompressTime.get(fileNameParamMethod) + decodingDuration / TIME_PRECISION * BLOCK_SIZE / block;
                    fileNameParamMethodToCompressedBits.put(fileNameParamMethod, newSize);
                    fileNameParamMethodToCompressTime.put(fileNameParamMethod, newCTime);
                    fileNameParamMethodToDecompressTime.put(fileNameParamMethod, newDTime);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(fileName, e);
        }
    }

    private void writeResult(String storeFile,
                             Map<String, Double> fileNameParamMethodToRatio,
                             Map<String, Double> fileNameParamMethodToCTime,
                             Map<String, Double> fileNameParamMethodToDTime,
                             Map<String, Long> fileNameParamToTotalBlock) {
        Map<String, List<Double>> methodToRatios = new TreeMap<>();
        Map<String, List<Double>> methodToCTimes = new HashMap<>();
        Map<String, List<Double>> methodToDTimes = new HashMap<>();

        for (String fileNameParamMethod : fileNameParamMethodToRatio.keySet()) {
            String fileName = fileNameParamMethod.split(",")[0];
            String param = fileNameParamMethod.split(",")[1];
            String method = fileNameParamMethod.split(",")[2];
            String fileNameParam = fileName + "," + param;
            if (fileName.equals(INIT_FILE)) {
                continue;
            }
            String paramMethod = param + "\t" + method;
            if (!methodToRatios.containsKey(paramMethod)) {
                methodToRatios.put(paramMethod, new ArrayList<>());
                methodToCTimes.put(paramMethod, new ArrayList<>());
                methodToDTimes.put(paramMethod, new ArrayList<>());
            }
            methodToRatios.get(paramMethod).add(fileNameParamMethodToRatio.get(fileNameParamMethod));
            methodToCTimes.get(paramMethod).add(fileNameParamMethodToCTime.get(fileNameParamMethod) / fileNameParamToTotalBlock.get(fileNameParam));
            methodToDTimes.get(paramMethod).add(fileNameParamMethodToDTime.get(fileNameParamMethod) / fileNameParamToTotalBlock.get(fileNameParam));
        }

        System.out.println("Average Performance");
        System.out.println("Param\tMethod\tRatio\tCTime\tDTime");
        for (String paramMethod : methodToRatios.keySet()) {
            System.out.print(paramMethod + "\t");
            System.out.print(methodToRatios.get(paramMethod).stream().mapToDouble(o -> o).average().orElse(0) + "\t");
            System.out.print(methodToCTimes.get(paramMethod).stream().mapToDouble(o -> o).average().orElse(0) + "\t");
            System.out.println(methodToDTimes.get(paramMethod).stream().mapToDouble(o -> o).average().orElse(0));
        }

        try {
            File file = new File(storeFile).getParentFile();
            if (!file.exists() && !file.mkdirs()) {
                throw new IOException("Create directory failed: " + file);
            }
            try (FileWriter writer = new FileWriter(storeFile, false)) {
                writer.write("Dataset, Param, Method, Ratio, CTime, DTime");
                writer.write("\r\n");
                // 遍历键，并写入对应的值
                for (String fileNameParamMethod : fileNameParamMethodToRatio.keySet()) {
                    String fileNameParam = fileNameParamMethod.split(",")[0] + "," + fileNameParamMethod.split(",")[1];
                    writer.write(fileNameParamMethod);
                    writer.write(",");
                    writer.write(fileNameParamMethodToRatio.get(fileNameParamMethod).toString());
                    writer.write(",");
                    writer.write(fileNameParamMethodToCTime.get(fileNameParamMethod) / fileNameParamToTotalBlock.get(fileNameParam) + "");
                    writer.write(",");
                    writer.write(fileNameParamMethodToDTime.get(fileNameParamMethod) / fileNameParamToTotalBlock.get(fileNameParam) + "");
                    writer.write("\r\n");
                }
                System.out.println("Done!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
