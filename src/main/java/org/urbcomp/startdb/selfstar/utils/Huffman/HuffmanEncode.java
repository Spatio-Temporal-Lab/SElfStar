package org.urbcomp.startdb.selfstar.utils.Huffman;

import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.util.Objects;
import java.util.PriorityQueue;

public class HuffmanEncode {
    // Map value -> <code, length>
    private static final Code[] huffmanCodes = new Code[18];
    private final int[] values;

    public HuffmanEncode(int[] values, int[] frequencies) {
        this.values = values;
        buildHuffmanTreeAndConToHashMap(values, frequencies);
    }

    public Code[] getHuffmanCodes(){
        return huffmanCodes;
    }

    private static void buildHuffmanTreeAndConToHashMap(int[] values, int[] frequencies) {
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
            newNode.children[0] = left;
            newNode.children[1] = right;
            nodePriorityQueue.add(newNode);
        }
        generateHuffmanCodes(nodePriorityQueue.peek(), 0, 0);
    }

    private static void generateHuffmanCodes(Node root, long code, int length) {
        if (root != null) {
            if (root.data != -Integer.MAX_VALUE) {
                huffmanCodes[root.data] = new Code(code, length);
            }
            generateHuffmanCodes(root.children[0], code << 1, length + 1);
            generateHuffmanCodes(root.children[1], (code << 1) | 1, length + 1);
        }
    }

    public static Node hashMapToTree(Code[] huffmanCodes) {
        Node root = new Node(-Integer.MAX_VALUE, 0);
        Node curNode = root;
        for (int value = 0; value < huffmanCodes.length; value++) {
            long code = huffmanCodes[value].value;
            int length = huffmanCodes[value].length;
            long signal;
            while (length != 0) {
                signal = (code >> (length - 1)) & 1;
                if (signal == 0) {
                    if (curNode.children[0] == null) {
                        curNode.children[0] = new Node(-Integer.MAX_VALUE, 0);
                    }
                    curNode = curNode.children[0];
                } else {
                    if (curNode.children[1] == null) {
                        curNode.children[1] = new Node(-Integer.MAX_VALUE, 0);
                    }
                    curNode = curNode.children[1];
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
            thisSize += out.writeInt(huffmanCodes[value].length, 5);
            thisSize += out.writeLong(huffmanCodes[value].value, huffmanCodes[value].length);
        }
        return thisSize;
    }
}