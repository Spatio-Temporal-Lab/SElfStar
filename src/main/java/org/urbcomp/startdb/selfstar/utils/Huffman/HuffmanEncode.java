package org.urbcomp.startdb.selfstar.utils.Huffman;

import javafx.util.Pair;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;

public class HuffmanEncode {
    // Map value -> <code, length>
    private static final HashMap<Integer, Pair<Long, Integer>> huffmanCodes = new HashMap<>();
    private final int[] values;

    public HuffmanEncode(int[] values, int[] frequencies) {
        this.values = values;
        buildHuffmanTreeAndConToHashMap(values, frequencies);
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
            Node newNode = new Node(-Integer.MAX_VALUE, left.frequency + Objects.requireNonNull(right).frequency);
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

    public static Node hashMapToTree(HashMap<Integer, Pair<Long, Integer>> huffmanCodes) {
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

    public int writeHuffmanCodes(OutputBitStream out) {
        int thisSize = 0;
        for (int value : values) {
            thisSize += out.writeInt(huffmanCodes.get(value).getValue(), 5);
            thisSize += out.writeLong(huffmanCodes.get(value).getKey(), huffmanCodes.get(value).getValue());
        }
        return thisSize;
    }
}