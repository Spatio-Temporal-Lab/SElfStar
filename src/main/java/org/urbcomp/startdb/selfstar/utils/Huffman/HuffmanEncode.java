package org.urbcomp.startdb.selfstar.utils.Huffman;

import org.urbcomp.startdb.selfstar.utils.InputBitStream;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.io.IOException;
import java.util.PriorityQueue;

public class HuffmanEncode {
    public static Code[] getHuffmanCodes(int[] frequencies) {
        Code[] huffmanCodes = new Code[frequencies.length];
        PriorityQueue<Node> nodePriorityQueue = new PriorityQueue<>(frequencies.length);

        // Construct priorityQueue
        for (int i = 0; i < frequencies.length; i++) {
            nodePriorityQueue.add(new Node(i, frequencies[i], 0));
        }

        // Construct huffman tree
        while (nodePriorityQueue.size() > 1) {
            Node left = nodePriorityQueue.poll();
            Node right = nodePriorityQueue.poll();
            @SuppressWarnings("all")
            int height = left.height > right.height ? left.height + 1 : right.height + 1;
            Node newNode = new Node(-1, left.frequency + right.frequency, height);
            newNode.children[0] = left;
            newNode.children[1] = right;
            nodePriorityQueue.add(newNode);
        }

        generateHuffmanCodes(huffmanCodes, nodePriorityQueue.peek(), 0, 0);

        return huffmanCodes;
    }


    private static void generateHuffmanCodes(Code[] huffmanCodes, Node root, int code, int length) {
        if (root != null) {
            if (root.data >= 0) {
                huffmanCodes[root.data] = new Code(code, length);
            }
            generateHuffmanCodes(huffmanCodes, root.children[0], code << 1, length + 1);
            generateHuffmanCodes(huffmanCodes, root.children[1], (code << 1) | 1, length + 1);
        }
    }

    public static Node buildHuffmanTree(Code[] huffmanCodes) {
        Node root = new Node(-1);
        Node curNode = root;
        for (int value = 0; value < huffmanCodes.length; value++) {
            int code = huffmanCodes[value].code;
            int length = huffmanCodes[value].length - 1;
            int signal;
            while (length >= 0) {
                signal = (code >> length) & 1;
                if (curNode.children[signal] == null) {
                    curNode.children[signal] = new Node(-1);
                }
                curNode = curNode.children[signal];
                --length;
            }
            curNode.data = value;
            curNode = root;
        }
        return root;
    }

    public static int writeHuffmanCodes(OutputBitStream out, Code[] huffmanCodes) {
        int maxLen = 0;
        for (Code code : huffmanCodes) {
            maxLen = Math.max(code.length, maxLen);
        }
        int[] logMap = {0, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4};  // minLength = 1, maxLength = 16, Math.ceil(log2{i})
        int bitsForLen = logMap[maxLen - 1];
        int thisSize = out.writeInt(bitsForLen, 3);    // 4 needs 4 bits
        for (Code huffmanCode : huffmanCodes) {
            thisSize += out.writeInt(huffmanCode.length - 1, bitsForLen);
            thisSize += out.writeInt(huffmanCode.code, huffmanCode.length);
        }
        return thisSize;
    }

    public static void readHuffmanCodes(InputBitStream in, Code[] codes) {
        try {
            int bitsForLen = in.readInt(3);
            for (int i = 0; i < codes.length; i++) {
                int length = in.readInt(bitsForLen) + 1;     // we should add 1 here
                int code = in.readInt(length);
                codes[i] = new Code(code, length);
            }
        } catch (IOException e) {
            throw new RuntimeException("IO error: " + e.getMessage());
        }
    }
}
