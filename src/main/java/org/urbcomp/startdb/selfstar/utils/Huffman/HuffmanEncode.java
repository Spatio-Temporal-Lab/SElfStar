package org.urbcomp.startdb.selfstar.utils.Huffman;

import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.util.PriorityQueue;

public class HuffmanEncode {
    // Map value -> <code, length>
    private final Code[] huffmanCodes = new Code[17];

    public HuffmanEncode(int[] frequencies) {
        buildHuffmanTreeAndConToHashMap(frequencies);
    }

    public Code[] getHuffmanCodes(){
        return huffmanCodes;
    }

    private void buildHuffmanTreeAndConToHashMap(int[] frequencies) {
        PriorityQueue<Node> nodePriorityQueue = new PriorityQueue<>(huffmanCodes.length);

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
        generateHuffmanCodes(nodePriorityQueue.peek(), 0, 0);
    }

    private void generateHuffmanCodes(Node root, int code, int length) {
        if (root != null) {
            if (root.data != -Integer.MAX_VALUE) {
                huffmanCodes[root.data] = new Code(code, length);
            }
            generateHuffmanCodes(root.children[0], code << 1, length + 1);
            generateHuffmanCodes(root.children[1], (code << 1) | 1, length + 1);
        }
    }

    public Node hashMapToTree(Code[] huffmanCodes) {
        Node root = new Node(-Integer.MAX_VALUE, 0);
        Node curNode = root;
        for (int value = 0; value < huffmanCodes.length; value++) {
            int code = huffmanCodes[value].value;
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

    public int writeHuffmanCodes(OutputBitStream out) {
        int thisSize = 0;
        for (Code huffmanCode : huffmanCodes) {
            thisSize += out.writeInt(huffmanCode.length, 5);
            thisSize += out.writeInt(huffmanCode.value, huffmanCode.length);
        }
        return thisSize;
    }
}
