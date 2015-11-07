package cn.paxos.pocket.btree;

public interface Tx<K extends Comparable<K>>
{

  void addNode(BTreeNode<K> node);

  void commit();

}
