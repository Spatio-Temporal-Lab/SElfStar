# Adaptive Encoding Strategies for Streaming Erasing-Based Lossless Floating-Point Compression

***
Based on the erasing method of ***Elf***, we improved it to get the ***ElfStar*** method. Compared with the ***Elf***
algorithm, the compression ratio has been significantly improved. In addition, ***SElfStar*** suitable for streaming
scenarios is also given.

## ElfStar & SElfStar feature

- ***ElfStar*** proposes the most advanced encoding strategy at present, and Huffman coding for β∗, optimizes the leading zeros, center bits
  and sharing conditions of ***Elf*** encoding.
- ***SElfStar*** is used in streaming scenarios and can achieve almost the same compression ratio as ***ElfStar***
  while enjoying higher efficiency.

## Project Structure

This project mainly includes the following various compression algorithms:

- The main code for the **XOR-based** algorithm is in the *org/urbcomp/startdb/compress/SElfStar* package.

- The main code for other **general compression** algorithms is in the *org/apache/hadoop/hbase/io/compress* package.

### XOR-based Structure

XOR-based includes *compressor* and *decompressor* packages. Each complete compression algorithm requires an
xorCompressor selected in the *compressor.xor* package. The same complete decompression algorithm also needs to select a
xorDecompressor in the *decompressor.xor* package

#### compressor package

This package includes 7 different preprocessing methods XOR-based compression and gives a standard **ICompressor**
interface. In addition, 15 different XOR encodings are placed in the *xor* package and gives a standard **
IXORCompressor**
interface.

- ElfCompressor: This class is the most primitive ***Elf*** algorithm.
- ElfPlusCompressor: This class ***Elf*** improves the storage of beta.
- ElfStarCompressor: This class is ***ElfStar***, which is processed in batch mode, and is also the algorithm for the
  optimal compression radio of the ***Elf*** series. Different XORCompressors can be selected for ablation test.
- ElfStarCompressorNoHuff: This class is ***ElfStar*** without Huffman coding.
- SElfStarCompressor: This class is ***SElfStar***, which is processed in stream mode.
- BaseCompressor: This class chooses different XORCompressors to implement Chimp, Chimp128 and Gorilla algorithms.
- SBaseCompressor: This class chooses different XORCompressors to implement Chimp, Chimp128 algorithms with approximation rules.

##### compressor.xor package

This package contains classes with different encoding methods and gives the standard interface IXORCompressor. The
following table shows the XORCompressor that can be selected by different Compressors and gives detailed information

<table>
  <tr>
    <th>Compressor</th>
    <th colspan="1">XORCompressor</th>
    <th>Detailed</th>
  </tr>
  <tr>
    <td rowspan="1">ElfCompressor</td>
    <td>ElfXORCompressor</td>
    <td>The original Elf algorithm.</td>
  </tr>
  <tr>
    <td rowspan="1">ElfPlusCompressor</td>
    <td>ElfPlusXORCompressor</td>
    <td>Optimized the Elf algorithm for beta storage.</td>
  </tr>
<tr>
    <td rowspan="5">ElfStarCompressor</td>
    <td>ElfStarXORCompressor</td>
    <td>Complete Elf* compression algorithm</td>
  </tr>
<tr>
    <td>ElfStarXORCompressorNoFRPruning</td>
    <td>Elf∗ without Front-Rear Pruning</td>
  </tr>
<tr>
    <td>ElfStarXORCompressorNoFRZPruning</td>
    <td>Elf∗ without Front-Rear Pruning and Zero Pruning</td>
  </tr>
<tr>
    <td>ElfStarXORCompressorNoFRZGPruning</td>
    <td>Elf∗ without Front-Rear Pruning, Zero Pruning and global pruning optimizations</td>
  </tr>
<tr>
    <td>ElfHuffXORCompressor</td>
    <td>Replacing the approximation rules in Elf∗ with Huffman trees</td>
  </tr>
  <tr>
    <td rowspan="3">ElfStarCompressorNoHuff</td>
    <td>ElfStarXORCompressorNoSC</td>
    <td>Elf∗ without Huffman coding, sharing condition and center bits optimizations</td>
  </tr> 
  <tr>
    <td>ElfStarXORCompressorNoS</td>
    <td>Elf∗ without Huffman coding and sharing condition optimizations</td>
  </tr>
  <tr>
    <td>ElfStarXORCompressor</td>
    <td>Elf∗ without Huffman coding for β∗</td>
  </tr>
  <tr>
    <td rowspan="1">SElfStarCompressor</td>
    <td>SElfXORCompressor</td>
    <td>Complete SElf* compression algorithm</td>
  </tr>
  <tr>
    <td rowspan="3">BaseCompressor</td>
    <td>GorillaXORCompressor</td>
    <td>Gorilla compression algorithm</td>
  </tr> 
  <tr>
    <td>ChimpXORCompressor</td>
    <td>Chimp compression algorithm</td>
  </tr>
  <tr>
    <td>ChimpNXORCompressor</td>
    <td>Chimp compression algorithm (set the parameter to 128)</td>
  </tr>
<tr>
    <td rowspan="2">SBaseCompressor</td>
    <td>ChimpAdaXORCompressor</td>
    <td>Chimp algorithm using approximation rules</td>
  </tr> 
  <tr>
    <td>ChimpNAdaXORCompressor</td>
    <td>Chimp algorithm using approximation rules (set the parameter to 128)</td>
  </tr>

</table>

#### decompressor package

This package includes 7 different preprocessing methods XOR-based decompression and gives a standard **IDecompressor**
interface. In addition, different XOR encodings are placed in the *xor* package and gives a standard **IXORDecompressor**
interface.

## TEST ElfStar & SElfStar

We recommend IntelliJ IDEA for developing projects.

### Prerequisites for testing

The following resources need to be downloaded and installed:

- Java 8 download: https://www.oracle.com/java/technologies/downloads/#java8
- IntelliJ IDEA download: https://www.jetbrains.com/idea/
- git download:https://git-scm.com/download

Download and install jdk-8, IntelliJ IDEA and git.

### Clone code

1. Open *IntelliJ IDEA*, find the *git* column, and select *Clone...*

2. In the *Repository URL* interface, *Version control* selects *git*

3. URL filling: *https://github.com/Spatio-Temporal-Lab/SElfStar.git*

### Set JDK

File -> Project Structure -> Project -> Project SDK -> *add SDK*

Click *JDK* to select the address where you want to download jdk-8

### Test

Select the test folder, which includes *BlockReader* class and *TestCompressor* class

#### test Structure

The *BlockReader*class includes methods for reading data.

The *TestCompressor* class includes the experimental methods that appear in various papers, and the experimental results
will be saved in the corresponding path.

The *TestBeta* class includes the beta experiment portion of the paper.

The *TestTransmit* class includes the transmission experiment part of the paper.

The *TestSingleCompressor* class includes the compression experiments for single-precision data in the paper.



