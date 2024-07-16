import com.github.Cwida.alp.ALPCompression;
import com.github.Cwida.alp.ALPDecompression;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.compress.xz.LzmaCodec;
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

public class TestBeta {
    private static final String INIT_FILE = "init.csv";
    private static final String STORE_FILE = "src/test/resources/result/resultBeta.csv";
    private static final int BLOCK_SIZE = 1000;
    private static final double TIME_PRECISION = 1000.0;

    private final Map<String, Long> fileNameParamToTotalBits = new HashMap<>();
    private final Map<String, Long> fileNameParamToTotalBlock = new HashMap<>();
    private final Map<String, Long> fileNameParamMethodToCompressedBits = new HashMap<>();
    private final Map<String, Double> fileNameParamMethodToCompressTime = new HashMap<>();
    private final Map<String, Double> fileNameParamMethodToDecompressTime = new HashMap<>();
    private final Map<String, Double> fileNameParamMethodToCompressedRatio = new TreeMap<>();// use TreeMap to keep the order

    @Test
    public void testBeta() {
        String fileName = "Air-sensor.csv";
        for (int j = 18; j >= 1; j--) {
            testALPCompressor(fileName, j);
            testXZCompressor(fileName, j);
            testFloatingCompressor(fileName, j);
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

    private void testFloatingCompressor(String fileName, int beta) {
        String fileNameParam = fileName + "," + beta;
        fileNameParamToTotalBits.put(fileNameParam, 0L);
        fileNameParamToTotalBlock.put(fileNameParam, 0L);
        ICompressor[] compressors = new ICompressor[]{
                new ElfCompressor(new ElfXORCompressor()),
                new ElfPlusCompressor(new ElfPlusXORCompressor()),
                new ElfStarCompressor(new ElfStarXORCompressor()),
                new SElfStarCompressor(new SElfStarXORCompressor()),
        };

        IDecompressor[] decompressors = new IDecompressor[]{
                new ElfDecompressor(new ElfXORDecompressor()),
                new ElfPlusDecompressor(new ElfPlusXORDecompressor()),
                new ElfStarDecompressor(new ElfStarXORDecompressor()),
                new SElfStarDecompressor(new SElfStarXORDecompressor()),

        };
        boolean firstMethod = true;
//        System.out.println(fileName);
        for (int i = 0; i < compressors.length; i++) {
            ICompressor compressor = compressors[i];
//            System.out.println(compressor.getKey());
            try (BlockReader br = new BlockReader(fileName, BLOCK_SIZE)) {
                List<Double> floatings;

                while ((floatings = br.nextBetaBlock(beta)) != null) {

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
                    String fileNameParamMethod = fileName + "," + beta + "," + compressor.getKey();
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
                e.printStackTrace();
                throw new RuntimeException(fileName, e);
            }
            firstMethod = false;
        }
    }

    private void testALPCompressor(String fileName, int beta) {
        long compressorBits;
        String fileNameParam = fileName + "," + beta;

        fileNameParamToTotalBits.put(fileNameParam, 0L);
        fileNameParamToTotalBlock.put(fileNameParam, 0L);
        double encodingDuration = 0;
        double decodingDuration = 0;
        try (BlockReader br = new BlockReader(fileName, BLOCK_SIZE)) {
            List<List<List<Double>>> RowGroups = new ArrayList<>();
            List<List<Double>> floatingsList = new ArrayList<>();
            List<Double> floatings;
            int RGsize = 100;
            while ((floatings = br.nextBetaBlock(beta)) != null) {
                if (floatings.size() != BLOCK_SIZE) {
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
            ALPCompression compressor = new ALPCompression(BLOCK_SIZE);
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
                fileNameParamMethodToCompressTime.put(fileNameParamMethod, encodingDuration / TIME_PRECISION * BLOCK_SIZE / BLOCK_SIZE);
                fileNameParamMethodToDecompressTime.put(fileNameParamMethod, decodingDuration / TIME_PRECISION * BLOCK_SIZE / BLOCK_SIZE);
            } else {
                long newSize = fileNameParamMethodToCompressedBits.get(fileNameParamMethod) + compressorBits;
                double newCTime = fileNameParamMethodToCompressTime.get(fileNameParamMethod) + encodingDuration / TIME_PRECISION * BLOCK_SIZE / BLOCK_SIZE;
                double newDTime = fileNameParamMethodToDecompressTime.get(fileNameParamMethod) + decodingDuration / TIME_PRECISION * BLOCK_SIZE / BLOCK_SIZE;
                fileNameParamMethodToCompressedBits.put(fileNameParamMethod, newSize);
                fileNameParamMethodToCompressTime.put(fileNameParamMethod, newCTime);
                fileNameParamMethodToDecompressTime.put(fileNameParamMethod, newDTime);
            }

        } catch (Exception e) {

            throw new RuntimeException(fileName, e);
        }
    }

    private static double[] toDoubleArray(byte[] byteArray) {
        int times = Double.SIZE / Byte.SIZE;
        double[] doubles = new double[byteArray.length / times];
        for (int i = 0; i < doubles.length; i++) {
            doubles[i] = ByteBuffer.wrap(byteArray, i * times, times).getDouble();
        }
        return doubles;
    }

    private void testXZCompressor(String fileName, int beta) {
        long compressorBits;
        String fileNameParam = fileName + "," + beta;
        fileNameParamToTotalBits.put(fileNameParam, 0L);
        fileNameParamToTotalBlock.put(fileNameParam, 0L);
        try (BlockReader br = new BlockReader(fileName, BLOCK_SIZE)) {
            List<Double> floatings;
            while ((floatings = br.nextBetaBlock(beta)) != null) {

                if (floatings.size() != BLOCK_SIZE) {
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
                    fileNameParamMethodToCompressTime.put(fileNameParamMethod, encodingDuration / TIME_PRECISION * BLOCK_SIZE / BLOCK_SIZE);
                    fileNameParamMethodToDecompressTime.put(fileNameParamMethod, decodingDuration / TIME_PRECISION * BLOCK_SIZE / BLOCK_SIZE);
                } else {
                    long newSize = fileNameParamMethodToCompressedBits.get(fileNameParamMethod) + compressorBits;
                    double newCTime = fileNameParamMethodToCompressTime.get(fileNameParamMethod) + encodingDuration / TIME_PRECISION * BLOCK_SIZE / BLOCK_SIZE;
                    double newDTime = fileNameParamMethodToDecompressTime.get(fileNameParamMethod) + decodingDuration / TIME_PRECISION * BLOCK_SIZE / BLOCK_SIZE;
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
            try (FileWriter writer = new FileWriter(storeFile, true)) {
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
