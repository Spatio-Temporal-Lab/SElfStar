package org.urbcomp.startdb.selfstar.utils.Huffman;

import java.util.Comparator;


public class Node implements Comparable<Node> {
    public int data;
    public int frequency;
    public int row;
    public int depth;
    public long code;
    public Node[] children = new Node[2];

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
                ", code=" + Long.toBinaryString(code) +
                '}';
    }

    @Override
    public int compareTo(Node o) {
        return this.frequency - o.frequency;
    }
}

class CustomComparator implements Comparator<Node> {

    @Override
    public int compare(Node o1, Node o2) {
        if (o1.getDepth() != o2.getDepth()) {
            return Integer.compare(o1.getDepth(), o2.getDepth());
        } else {
            return Integer.compare(o1.getData(), o2.getData());
        }
    }
}