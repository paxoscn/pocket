package cn.paxos.pocket.btree;

public interface Disk<K extends Comparable<K>, V>
{

  Object createNode(BTreeNode<K> node);

  BTreeNode<K> getRoot();

  BTreeNode<K> getNode(Object nodeId);

  void setRoot(BTreeNode<K> node);

  BTreeNode<K> getChild(BTreeInnerNode<K> node, int index);

  void setParent(BTreeNode<K> node, Object parentId);

  void setChild(BTreeInnerNode<K> node, int index, BTreeNode<K> child);

  void setKey(BTreeNode<K> node, int index, K key);

  void setKeyCount(BTreeNode<K> node, int newKeyCount);

  void setValue(BTreeLeafNode<K, V> node, int index, V value);

  void setLeftSibling(BTreeNode<K> node, Object siblingId);

  void setRightSibling(BTreeNode<K> node, Object siblingId);

  Tx<K> getTx();

}
