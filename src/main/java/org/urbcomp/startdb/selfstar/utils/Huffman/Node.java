package org.urbcomp.startdb.selfstar.utils.Huffman;

public class Node implements Comparable<Node> {
    public int data;
    public int frequency;
    public Node[] children = new Node[2];
    public int height;  // leaf node is 0

    public Node(int data) {
        this.data = data;
    }

    // if data < 0, it means it is an internal node
    public Node(int data, int frequency, int height) {
        this.data = data;
        this.frequency = frequency;
        this.height = height;
    }

    @Override
    public String toString() {
        return "Node{" +
                "data=" + data +
                ", frequency=" + frequency +
                '}';
    }

    @Override
    public int compareTo(Node o) {
        return this.frequency != o.frequency ? this.frequency - o.frequency : this.height - o.height;
    }
}