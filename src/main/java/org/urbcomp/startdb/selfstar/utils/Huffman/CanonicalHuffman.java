package org.urbcomp.startdb.selfstar.utils.Huffman;

import org.urbcomp.startdb.selfstar.decompressor.ElfStarCanonicalHuffmanDecompressor;
import org.urbcomp.startdb.selfstar.utils.InputBitStream;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.io.IOException;
import java.util.*;

public class CanonicalHuffman {
    private static final int CODE_LEN = 5;
    private static final List<Node> nodeList = new ArrayList<>();
    private final int num;
    private final int[] values;
    private final int[] frequencies;

    public CanonicalHuffman(int[] values, int[] frequencies) {
        this.values = values;
        this.frequencies = frequencies;
        num = values.length;
        calculateDepth();
        generateCode(nodeList);
    }

    public static void generateCode(List<Node> nodeList) {
        nodeList.sort(new CustomComparator());
        int num = nodeList.size();
        int lastDepth = nodeList.get(0).depth;
        long lastCode = 0;
        nodeList.get(0).code = 0;
        for (int i = 1; i < num; i++) {
            int shift = nodeList.get(i).depth - lastDepth;
            long code = lastCode;
            if (shift == 0) {
                code += 1;
            } else {
                code = (lastCode + 1) << shift;
            }
            nodeList.get(i).code = code;
            lastCode = code;
            lastDepth = nodeList.get(i).depth;
        }
    }

    public static void main(String[] args) throws IOException {
        int[] values = new int[]{
                0, 1, 2, 3, 4, 5, 6, 7
        };
        int[] f = new int[]{
                11, 5, 7, 8, 323, 22, 1, 22
        };
        CanonicalHuffman ch = new CanonicalHuffman(values, f);
        OutputBitStream out = new OutputBitStream(new byte[100]);
        ch.writeHuffmanCodes(out);

        List<Node> huffmanCode = new ArrayList<>();
        InputBitStream in = new InputBitStream(Arrays.copyOf(out.getBuffer(), 4));
        int maxCodeLen = 0;
        for (int state : values) {
            Node node = new Node(state);
            node.depth = in.readInt(3);
            huffmanCode.add(node);
            if (node.depth > maxCodeLen) {
                maxCodeLen = node.depth;
            }
        }
        CanonicalHuffman.generateCode(huffmanCode);
        for(Node node:huffmanCode){
//            System.out.println(node);
        }
        System.out.println("-------------------------------");
        Node[] lookup = ElfStarCanonicalHuffmanDecompressor.generateLookupArray(huffmanCode, maxCodeLen);
        for(int i=0;i<lookup.length;i++){
//            System.out.println(Integer.toBinaryString(i)+"  "+lookup[i]);
        }


    }
    public void calculateDepth() {
        PriorityQueue<Node> nodePriorityQueue = new PriorityQueue<>();
        // Construct priorityQueue
        for (int i = 0; i < num; i++) {
            Node valueNode = new Node(values[i], frequencies[i]);
            nodePriorityQueue.add(valueNode);
        }
        int iRow = 1;
        while (nodePriorityQueue.size() > 1) {
            Node node1 = nodePriorityQueue.poll();
            Node node2 = nodePriorityQueue.poll();
            if (node1.row == 0 && Objects.requireNonNull(node2).row == 0) {
                node1.row = iRow;
                node1.depth++;
                node2.row = iRow;
                node2.depth++;
                nodeList.add(node1);
                nodeList.add(node2);
            } else if ((node1.row == 0 && node2.row != 0)) {
                addNode(iRow, node1, node2);
            } else if (node1.row != 0 && Objects.requireNonNull(node2).row == 0) {
                addNode(iRow, node2, node1);
            } else if (node1.row != 0 && node2.row != 0) {
                for (Node node : nodeList)
                    if (node.row == node1.row || node.row == node2.row) {
                        node.row = iRow;
                        node.depth++;
                    }
            }
            Node newNode = new Node(0, node1.frequency + node2.frequency);
            newNode.row = iRow;
            nodePriorityQueue.add(newNode);
            iRow++;
        }
    }

    private void addNode(int iRow, Node node1, Node node2) {
        node1.row = iRow;
        node1.depth++;
        nodeList.add(node1);
        int tempRow = node2.row;
        for (Node node : nodeList) {
            if (node.row == tempRow) {
                node.row = iRow;
                node.depth++;
            }
        }
    }

    public List<Node> getNodeList() {
        return nodeList;
    }

    public int writeHuffmanCodes(OutputBitStream out) {
        nodeList.sort(Comparator.comparingInt(Node::getData));
        int thisSize = 0;
        for (int i = 0; i < num; i++) {
            thisSize += out.writeInt(nodeList.get(i).depth, CODE_LEN);
        }
        return thisSize;
    }
}
