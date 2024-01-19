package org.urbcomp.startdb.selfstar.utils.Huffman;

public class Node implements Comparable<Node> {
    public int data;
    public int frequency;
    public int row;
    public int depth;
    public long code;
    public Node left, right;

    public Node(int data, int frequency) {
        this.data = data;
        this.frequency = frequency;
        row = 0;
    }

    public Node(int data) {
        this.data = data;
    }


    public int getData() {
        return data;
    }

    public int getDepth() {
        return depth;
    }

    @Override
    public String toString() {
        return "Node{" +
                "data=" + data +
                ", frequency=" + frequency +
                ", depth=" + depth +
                ", code=" + code +
                '}';
    }

    @Override
    public int compareTo(Node o) {
        return this.frequency - o.frequency;
    }
}