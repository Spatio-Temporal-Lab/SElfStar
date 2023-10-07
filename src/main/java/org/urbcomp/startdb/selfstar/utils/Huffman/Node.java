package org.urbcomp.startdb.selfstar.utils.Huffman;

public class Node implements Comparable<Node> {
    public int data;
    public int frequency;
    public Node left, right;

    public Node(int data, int frequency) {
        this.data = data;
        this.frequency = frequency;
    }

    @Override
    public int compareTo(Node o) {
        return this.frequency - o.frequency;
    }
}