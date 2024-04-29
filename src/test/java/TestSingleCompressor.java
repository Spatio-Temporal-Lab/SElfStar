import com.github.Cwida.alp.ALPCompression32;
import com.github.Cwida.alp.ALPDecompression32;
import com.github.Tranway.buff.BuffCompressor32;
import com.github.Tranway.buff.BuffDecompressor32;
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
import org.urbcomp.startdb.selfstar.compressor32.*;
import org.urbcomp.startdb.selfstar.compressor32.xor.*;
import org.urbcomp.startdb.selfstar.decompressor32.*;
import org.urbcomp.startdb.selfstar.decompressor32.xor.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSingleCompressor {
    private static final String STORE_FILE = "src/test/resources/result/result32.csv";
    private static final double TIME_PRECISION = 1000.0;
    private static final int BLOCK_SIZE = 1000;
    private static final int NO_PARAM = 0;
    private static final String INIT_FILE = "init.csv";     // warm up memory and cpu
    private final String[] fileNames = {
            INIT_FILE,
            "Air-pressure.csv",
            "Bird-migration.csv",
            "Blockchain-tr.csv",
            "City-lat.csv",
            "City-lon.csv",
            "City-temp.csv",
            "Dew-point-temp.csv",
            "electric_vehicle_charging.csv",
            "Food-price.csv",
            "IR-bio-temp.csv",
            "PM10-dust.csv",
            "SSD-bench.csv",
            "Stocks-DE.csv",
            "Stocks-UK.csv",
            "Stocks-USA.csv",
            "Wind-Speed.csv",
    };

    private final Map<String, Long> fileNameParamToTotalBits = new HashMap<>();
    private final Map<String, Long> fileNameParamToTotalBlock = new HashMap<>();
    private final Map<String, Long> fileNameParamMethodToCompressedBits = new HashMap<>();
    private final Map<String, Double> fileNameParamMethodToCompressTime = new HashMap<>();
    private final Map<String, Double> fileNameParamMethodToDecompressTime = new HashMap<>();
    private final Map<String, Double> fileNameParamMethodToCompressedRatio = new TreeMap<>();// use TreeMap to keep the order

    @Test
    public void testAllCompressor() {
        for (String fileName : fileNames) {
            testALPCompressor(fileName);
            testBuffCompressor(fileName);
            testXZCompressor(fileName);
            testZstdCompressor(fileName);
            testSnappyCompressor(fileName);
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


    private void testFloatingCompressor(String fileName) {

        String fileNameParam = fileName + "," + NO_PARAM;
        fileNameParamToTotalBits.put(fileNameParam, 0L);
        fileNameParamToTotalBlock.put(fileNameParam, 0L);
        ICompressor32[] compressors = new ICompressor32[]{
                new BaseCompressor32(new ChimpXORCompressor32()),
                new BaseCompressor32(new ChimpNXORCompressor32(128)),
                new BaseCompressor32(new GorillaXORCompressor32()),
                new ElfCompressor32(new ElfXORCompressor32()),
                new ElfPlusCompressor32(new ElfPlusXORCompressor32()),
                new ElfStarCompressor32(new ElfStarXORCompressor32()),
                new SElfStarCompressor32(new SElfStarXORCompressor32()),
                new ElfStarHuffmanCompressor32(new ElfStarXORCompressor32()),
                new SElfStarHuffmanCompressor32(new SElfStarXORCompressor32())
        };

        IDecompressor32[] decompressors = new IDecompressor32[]{
                new BaseDecompressor32(new ChimpXORDecompressor32()),
                new BaseDecompressor32(new ChimpNXORDecompressor32(128)),
                new BaseDecompressor32(new GorillaXORDecompressor32()),
                new ElfDecompressor32(new ElfXORDecompressor32()),
                new ElfPlusDecompressor32(new ElfPlusXORDecompressor32()),
                new ElfStarDecompressor32(new ElfStarXORDecompressor32()),
                new ElfStarDecompressor32(new SElfStarXORDecompressor32()),
                new ElfStarHuffmanDecompressor32(new ElfStarXORDecompressor32()),
                new SElfStarHuffmanDecompressor32(new SElfStarXORDecompressor32())
        };
        boolean firstMethod = true;
        for (int i = 0; i < compressors.length; i++) {
            ICompressor32 compressor = compressors[i];
            try (BlockReader br = new BlockReader(fileName, BLOCK_SIZE)) {
                List<Float> floatings;

                while ((floatings = br.nextSingleBlock()) != null) {

                    double compressTime = 0;
                    double decompressTime;
                    if (floatings.size() != BLOCK_SIZE) {
                        break;
                    }
                    if (firstMethod) {
                        fileNameParamToTotalBits.put(fileNameParam, fileNameParamToTotalBits.get(fileNameParam) + floatings.size() * 32L);
                        fileNameParamToTotalBlock.put(fileNameParam, fileNameParamToTotalBlock.get(fileNameParam) + 1);
                    }
                    double start = System.nanoTime();
                    floatings.forEach(compressor::addValue);
                    compressor.close();
                    compressTime += (System.nanoTime() - start) / TIME_PRECISION;
                    IDecompressor32 decompressor = decompressors[i];
                    decompressor.setBytes(compressor.getBytes());

                    start = System.nanoTime();
                    List<Float> deValues = decompressor.decompress();
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

    private void testALPCompressor(String fileName) {
        long compressorBits;
        String fileNameParam = fileName + "," + NO_PARAM;
        fileNameParamToTotalBits.put(fileNameParam, 0L);
        fileNameParamToTotalBlock.put(fileNameParam, 0L);
        double encodingDuration = 0;
        double decodingDuration = 0;
        try (BlockReader br = new BlockReader(fileName, BLOCK_SIZE)) {
            List<List<List<Float>>> RowGroups = new ArrayList<>();
            List<List<Float>> floatingsList = new ArrayList<>();
            List<Float> floatings;
            int RGsize = 100;
            while ((floatings = br.nextSingleBlock()) != null) {
                if (floatings.size() != BLOCK_SIZE) {
                    break;
                }
                floatingsList.add(new ArrayList<>(floatings));
                fileNameParamToTotalBits.put(fileNameParam, fileNameParamToTotalBits.get(fileNameParam) + floatings.size() * 32L);
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
            ALPCompression32 compressor = new ALPCompression32();
            for (List<List<Float>> rowGroup : RowGroups) {
                compressor.entry(rowGroup);
                compressor.reset();
            }
            compressor.flush();
            encodingDuration += System.nanoTime() - start;

            byte[] result = compressor.getOut();

            start = System.nanoTime();
            ALPDecompression32 decompressor = new ALPDecompression32(result);

            List<List<float[]>> deValues = new ArrayList<>();
            for (int i = 0; i < RowGroups.size(); i++) {
                List<float[]> deValue = decompressor.entry();
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
            String fileNameParamMethod = fileNameParam + "," + "ALP32";
            if (!fileNameParamMethodToCompressedBits.containsKey(fileNameParamMethod)) {
                fileNameParamMethodToCompressedBits.put(fileNameParamMethod, compressorBits);
                fileNameParamMethodToCompressTime.put(fileNameParamMethod, encodingDuration / TIME_PRECISION);
                fileNameParamMethodToDecompressTime.put(fileNameParamMethod, decodingDuration / TIME_PRECISION);
            } else {
                long newSize = fileNameParamMethodToCompressedBits.get(fileNameParamMethod) + compressorBits;
                double newCTime = fileNameParamMethodToCompressTime.get(fileNameParamMethod) + encodingDuration / TIME_PRECISION;
                double newDTime = fileNameParamMethodToDecompressTime.get(fileNameParamMethod) + decodingDuration / TIME_PRECISION;
                fileNameParamMethodToCompressedBits.put(fileNameParamMethod, newSize);
                fileNameParamMethodToCompressTime.put(fileNameParamMethod, newCTime);
                fileNameParamMethodToDecompressTime.put(fileNameParamMethod, newDTime);
            }

        } catch (Exception e) {
            throw new RuntimeException(fileName, e);
        }
    }


    private void testBuffCompressor(String fileName) {
        long compressorBits;
        String fileNameParam = fileName + "," + NO_PARAM;
        fileNameParamToTotalBits.put(fileNameParam, 0L);
        fileNameParamToTotalBlock.put(fileNameParam, 0L);
        try (BlockReader br = new BlockReader(fileName, BLOCK_SIZE)) {
            List<Float> floatings;
            while ((floatings = br.nextSingleBlock()) != null) {
                if (floatings.size() != BLOCK_SIZE) {
                    break;
                }
                float[] values = new float[floatings.size()];
                for (int i = 0; i < floatings.size(); i++) {
                    values[i] = floatings.get(i);
                }
                fileNameParamToTotalBits.put(fileNameParam, fileNameParamToTotalBits.get(fileNameParam) + floatings.size() * 64L);
                fileNameParamToTotalBlock.put(fileNameParam, fileNameParamToTotalBlock.get(fileNameParam) + 1L);
                double encodingDuration = 0;
                double decodingDuration = 0;
                BuffCompressor32 compressor = new BuffCompressor32();
                // Compress
                long start = System.nanoTime();
                compressor.compress(values);
                encodingDuration += System.nanoTime() - start;
                compressorBits = compressor.getSize();

                byte[] result = compressor.getOut();
                BuffDecompressor32 decompressor = new BuffDecompressor32(result);

                start = System.nanoTime();

                float[] uncompressed = decompressor.decompress();
                decodingDuration += System.nanoTime() - start;

                // Decompressed bytes should equal the original
                for (int i = 0; i < floatings.size(); i++) {
                    assertEquals(floatings.get(i), uncompressed[i], "Value did not match");
                }
                String fileNameParamMethod = fileNameParam + "," + "Buff32";
                if (!fileNameParamMethodToCompressedBits.containsKey(fileNameParamMethod)) {
                    fileNameParamMethodToCompressedBits.put(fileNameParamMethod, compressorBits);
                    fileNameParamMethodToCompressTime.put(fileNameParamMethod, encodingDuration / TIME_PRECISION);
                    fileNameParamMethodToDecompressTime.put(fileNameParamMethod, decodingDuration / TIME_PRECISION);
                } else {
                    long newSize = fileNameParamMethodToCompressedBits.get(fileNameParamMethod) + compressorBits;
                    double newCTime = fileNameParamMethodToCompressTime.get(fileNameParamMethod) + encodingDuration / TIME_PRECISION;
                    double newDTime = fileNameParamMethodToDecompressTime.get(fileNameParamMethod) + decodingDuration / TIME_PRECISION;
                    fileNameParamMethodToCompressedBits.put(fileNameParamMethod, newSize);
                    fileNameParamMethodToCompressTime.put(fileNameParamMethod, newCTime);
                    fileNameParamMethodToDecompressTime.put(fileNameParamMethod, newDTime);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(fileName, e);
        }
    }


    private void testXZCompressor(String fileName) {
        long compressorBits;
        String fileNameParam = fileName + "," + NO_PARAM;
        fileNameParamToTotalBits.put(fileNameParam, 0L);
        fileNameParamToTotalBlock.put(fileNameParam, 0L);
        try (BlockReader br = new BlockReader(fileName, BLOCK_SIZE)) {
            List<Float> floatings;
            while ((floatings = br.nextSingleBlock()) != null) {

                if (floatings.size() != BLOCK_SIZE) {
                    break;
                }
                fileNameParamToTotalBits.put(fileNameParam, fileNameParamToTotalBits.get(fileNameParam) + floatings.size() * 32L);
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
                for (float d : floatings) {
                    bb.putFloat(d);
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
                float[] uncompressed = toFloatArray(plain);
                decodingDuration += System.nanoTime() - start;

                // Decompressed bytes should equal the original
                for (int i = 0; i < floatings.size(); i++) {
                    assertEquals(floatings.get(i), uncompressed[i], "Value did not match");
                }
                String fileNameParamMethod = fileNameParam + "," + "Xz";
                if (!fileNameParamMethodToCompressedBits.containsKey(fileNameParamMethod)) {
                    fileNameParamMethodToCompressedBits.put(fileNameParamMethod, compressorBits);
                    fileNameParamMethodToCompressTime.put(fileNameParamMethod, encodingDuration / TIME_PRECISION);
                    fileNameParamMethodToDecompressTime.put(fileNameParamMethod, decodingDuration / TIME_PRECISION);
                } else {
                    long newSize = fileNameParamMethodToCompressedBits.get(fileNameParamMethod) + compressorBits;
                    double newCTime = fileNameParamMethodToCompressTime.get(fileNameParamMethod) + encodingDuration / TIME_PRECISION;
                    double newDTime = fileNameParamMethodToDecompressTime.get(fileNameParamMethod) + decodingDuration / TIME_PRECISION;
                    fileNameParamMethodToCompressedBits.put(fileNameParamMethod, newSize);
                    fileNameParamMethodToCompressTime.put(fileNameParamMethod, newCTime);
                    fileNameParamMethodToDecompressTime.put(fileNameParamMethod, newDTime);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(fileName, e);
        }
    }

    private void testZstdCompressor(String fileName) {
        long compressorBits;
        String fileNameParam = fileName + "," + NO_PARAM;
        fileNameParamToTotalBits.put(fileNameParam, 0L);
        fileNameParamToTotalBlock.put(fileNameParam, 0L);
        try (BlockReader br = new BlockReader(fileName, BLOCK_SIZE)) {
            List<Float> floatings;
            while ((floatings = br.nextSingleBlock()) != null) {

                if (floatings.size() != BLOCK_SIZE) {
                    break;
                }

                fileNameParamToTotalBits.put(fileNameParam, fileNameParamToTotalBits.get(fileNameParam) + floatings.size() * 32L);
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
                for (float d : floatings) {
                    bb.putFloat(d);
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
                float[] uncompressed = toFloatArray(plain);
                decodingDuration += System.nanoTime() - start;
                // Decompressed bytes should equal the original
                for (int i = 0; i < floatings.size(); i++) {
                    assertEquals(floatings.get(i), uncompressed[i], "Value did not match");
                }
                String fileNameMethod = fileNameParam + "," + "Zstd";
                if (!fileNameParamMethodToCompressedBits.containsKey(fileNameMethod)) {
                    fileNameParamMethodToCompressedBits.put(fileNameMethod, compressorBits);
                    fileNameParamMethodToCompressTime.put(fileNameMethod, encodingDuration / TIME_PRECISION);
                    fileNameParamMethodToDecompressTime.put(fileNameMethod, decodingDuration / TIME_PRECISION);
                } else {
                    long newSize = fileNameParamMethodToCompressedBits.get(fileNameMethod) + compressorBits;
                    double newCTime = fileNameParamMethodToCompressTime.get(fileNameMethod) + encodingDuration / TIME_PRECISION;
                    double newDTime = fileNameParamMethodToDecompressTime.get(fileNameMethod) + decodingDuration / TIME_PRECISION;
                    fileNameParamMethodToCompressedBits.put(fileNameMethod, newSize);
                    fileNameParamMethodToCompressTime.put(fileNameMethod, newCTime);
                    fileNameParamMethodToDecompressTime.put(fileNameMethod, newDTime);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(fileName, e);
        }
    }

    private void testSnappyCompressor(String fileName) {
        long compressorBits;
        String fileNameParam = fileName + "," + NO_PARAM;
        fileNameParamToTotalBits.put(fileNameParam, 0L);
        fileNameParamToTotalBlock.put(fileNameParam, 0L);
        try (BlockReader br = new BlockReader(fileName, BLOCK_SIZE)) {
            List<Float> floatings;
            while ((floatings = br.nextSingleBlock()) != null) {
                if (floatings.size() != BLOCK_SIZE) {
                    break;
                }

                fileNameParamToTotalBits.put(fileNameParam, fileNameParamToTotalBits.get(fileNameParam) + floatings.size() * 32L);
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
                for (float d : floatings) {
                    bb.putFloat(d);
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
                float[] uncompressed = toFloatArray(plain);
                decodingDuration += System.nanoTime() - start;

                // Decompressed bytes should equal the original
                for (int i = 0; i < floatings.size(); i++) {
                    assertEquals(floatings.get(i), uncompressed[i], "Value did not match");
                }

                System.gc();
                String fileNameParamMethod = fileNameParam + "," + "Snappy";
                if (!fileNameParamMethodToCompressedBits.containsKey(fileNameParamMethod)) {
                    fileNameParamMethodToCompressedBits.put(fileNameParamMethod, compressorBits);
                    fileNameParamMethodToCompressTime.put(fileNameParamMethod, encodingDuration / TIME_PRECISION);
                    fileNameParamMethodToDecompressTime.put(fileNameParamMethod, decodingDuration / TIME_PRECISION);
                } else {
                    long newSize = fileNameParamMethodToCompressedBits.get(fileNameParamMethod) + compressorBits;
                    double newCTime = fileNameParamMethodToCompressTime.get(fileNameParamMethod) + encodingDuration / TIME_PRECISION;
                    double newDTime = fileNameParamMethodToDecompressTime.get(fileNameParamMethod) + decodingDuration / TIME_PRECISION;
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

    public static float[] toFloatArray(byte[] byteArray) {
        int times = Float.SIZE / Byte.SIZE;
        float[] floats = new float[byteArray.length / times];
        for (int i = 0; i < floats.length; i++) {
            floats[i] = ByteBuffer.wrap(byteArray, i * times, times).getFloat();
        }
        return floats;
    }

}
