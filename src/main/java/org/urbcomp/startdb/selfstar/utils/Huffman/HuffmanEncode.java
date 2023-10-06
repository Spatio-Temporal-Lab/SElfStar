package org.urbcomp.startdb.selfstar.utils.Huffman;

import javafx.util.Pair;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.util.*;

//public class HuffmanEncode {
//
//    public static void main(String[] args) {
////        int[] value = new int[]{-16, -15, -14, -13, -12, -11, -10, -9, -8, -7, -6, -5, -4, -3, -2, -1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
////        int[] f = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 851, 2046, 2209, 5752, 34949, 245614, 245331, 35085, 5692, 2262, 2031, 848, 1, 1, 0, 0, 0, 0, 0, 0, 0};
//        int[] values = new int[]{1, 2, 3, 4, 5};
//        int[] f = new int[]{10, 3, 2, 0, 6};
//        HuffmanEncoder.buildHuffmanTreeAndConToHashMap(values, f);
//        Node node = HuffmanEncoder.hashMapToTree();
//        System.out.println(node.right.left.left.left.data);
//    }
//
//}

public class HuffmanEncode {
    // Map value -> <code, length>
    private static final HashMap<Integer, Pair<Long, Integer>> huffmanCodes = new HashMap<>();
    public static int[] values = new int[]{1, 2, 3, 4, 5};

    public HuffmanEncode(int[] values, int[] frequencies){
        buildHuffmanTreeAndConToHashMap(values,frequencies);
    }

    public HashMap<Integer, Pair<Long, Integer>> getHuffmanCodes(){
        return huffmanCodes;
    }

    public static void buildHuffmanTreeAndConToHashMap(int[] values, int[] frequencies) {
        PriorityQueue<Node> nodePriorityQueue = new PriorityQueue<>();

        // Construct priorityQueue
        for (int i = 0; i < values.length; i++) {
            nodePriorityQueue.add(new Node(values[i], frequencies[i]));
        }

        // Construct huffman tree
        while (nodePriorityQueue.size() > 1) {
            Node left = nodePriorityQueue.poll();
            Node right = nodePriorityQueue.poll();
            Node newNode = new Node(-Integer.MAX_VALUE, left.frequency + right.frequency);
            newNode.left = left;
            newNode.right = right;
            nodePriorityQueue.add(newNode);
        }

        generateHuffmanCodes(nodePriorityQueue.peek(), 0, 0);
    }

    private static void generateHuffmanCodes(Node root, long code, int length) {
        if (root != null) {
            if (root.data != -Integer.MAX_VALUE) {
                huffmanCodes.put(root.data, new Pair<>(code, length));
            }
            generateHuffmanCodes(root.left, code << 1, length + 1);
            generateHuffmanCodes(root.right, (code << 1) | 1, length + 1);
        }
    }

    public int writeHuffmanCodes(OutputBitStream out) {
        int thisSize = 0;
        for (int value : values) {
            thisSize += out.writeLong(huffmanCodes.get(value).getValue(), 5);
            thisSize += out.writeLong(huffmanCodes.get(value).getKey(), huffmanCodes.get(value).getValue());
        }
        return thisSize;
    }

    public static String compress(int[] values) {
        StringBuilder compressedData = new StringBuilder();
        for (int value : values) {
            compressedData.append(huffmanCodes.get(value));
        }
        return compressedData.toString();
    }

    public static Node hashMapToTree() {
        Node root = new Node(-Integer.MAX_VALUE, 0);
        Node curNode = root;
        for (Map.Entry<Integer, Pair<Long, Integer>> valueToCodeAndLen : huffmanCodes.entrySet()) {
            int value = valueToCodeAndLen.getKey();
            long code = valueToCodeAndLen.getValue().getKey();
            int length = valueToCodeAndLen.getValue().getValue();
            long signal;
            while (length != 0) {
                signal = (code >> (length - 1)) & 1;
                if (signal == 0) {
                    if (curNode.left == null) {
                        curNode.left = new Node(-Integer.MAX_VALUE, 0);
                    }
                    curNode = curNode.left;
                } else {
                    if (curNode.right == null) {
                        curNode.right = new Node(-Integer.MAX_VALUE, 0);
                    }
                    curNode = curNode.right;
                }
                length -= 1;
            }
            curNode.data = value;
            curNode = root;
        }
        return root;
    }

    public static int[] decompress(String compressedData, Node root) {
        List<Integer> decompressedData = new ArrayList<>();
        Node current = root;
        for (char bit : compressedData.toCharArray()) {
            if (bit == '0') {
                current = current.left;
            } else if (bit == '1') {
                current = current.right;
            }
            if (current.data != -1) {
                decompressedData.add(current.data);
                current = root;
            }
        }

        // Convert the List<Integer> to int[]
        int[] result = new int[decompressedData.size()];
        for (int i = 0; i < decompressedData.size(); i++) {
            result[i] = decompressedData.get(i);
        }
        return result;
    }
}