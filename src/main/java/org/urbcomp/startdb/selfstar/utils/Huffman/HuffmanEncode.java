package org.urbcomp.startdb.selfstar.utils.Huffman;

import org.urbcomp.startdb.selfstar.utils.InputBitStream;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.io.IOException;
import java.util.PriorityQueue;

public class HuffmanEncode {
    public static Code[] getHuffmanCodes(int[] frequencies){
        Code[] huffmanCodes = new Code[frequencies.length];
        PriorityQueue<Node> nodePriorityQueue = new PriorityQueue<>(frequencies.length);

        // Construct priorityQueue
        for (int i = 0; i < frequencies.length; i++) {
            nodePriorityQueue.add(new Node(i, frequencies[i]));
        }

        // Construct huffman tree
        while (nodePriorityQueue.size() > 1) {
            Node left = nodePriorityQueue.poll();
            Node right = nodePriorityQueue.poll();
            @SuppressWarnings("all")
            Node newNode = new Node(-Integer.MAX_VALUE, left.frequency + right.frequency);
            newNode.children[0] = left;
            newNode.children[1] = right;
            nodePriorityQueue.add(newNode);
        }

        generateHuffmanCodes(huffmanCodes, nodePriorityQueue.peek(), 0, 0);

        return huffmanCodes;
    }


    private static void generateHuffmanCodes(Code[] huffmanCodes, Node root, int code, int length) {
        if (root != null) {
            if (root.data != -Integer.MAX_VALUE) {
                huffmanCodes[root.data] = new Code(code, length);
            }
            generateHuffmanCodes(huffmanCodes, root.children[0], code << 1, length + 1);
            generateHuffmanCodes(huffmanCodes, root.children[1], (code << 1) | 1, length + 1);
        }
    }

    public static Node hashMapToTree(Code[] huffmanCodes) {
        Node root = new Node(-Integer.MAX_VALUE, 0);
        Node curNode = root;
        for (int value = 0; value < huffmanCodes.length; value++) {
            int code = huffmanCodes[value].code;
            int length = huffmanCodes[value].length - 1;
            int signal;
            while (length >= 0) {
                signal = (code >> length) & 1;
                if (curNode.children[signal] == null) {
                    curNode.children[signal] = new Node(-Integer.MAX_VALUE, 0);
                }
                curNode = curNode.children[signal];
                length -= 1;
            }
            curNode.data = value;
            curNode = root;
        }
        return root;
    }

    public static int writeHuffmanCodes(OutputBitStream out, Code[] huffmanCodes) {
        int thisSize = 0;
        for (Code huffmanCode : huffmanCodes) {
            thisSize += out.writeInt(huffmanCode.length - 1, 4);       // minLength = 1, maxLength = 16, so 4 bits is enough
            thisSize += out.writeInt(huffmanCode.code, huffmanCode.length);
        }
        return thisSize;
    }

    public static void readHuffmanCodes(InputBitStream in, Code[] codes) {
        try {
            for (int i = 0; i < codes.length; i++) {
                int length = in.readInt(4) + 1;     // we should add 1 here
                int code = in.readInt(length);
                codes[i] = new Code(code, length);
            }
        } catch (IOException e) {
            throw new RuntimeException("IO error: " + e.getMessage());
        }
    }
}
