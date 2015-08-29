package cn.paxos.pocket.btree;

public class DummyDisk<K extends Comparable<K>, V> implements
    Disk<K, V>
{

  private BTreeNode<K> root;

  public DummyDisk()
  {
    this.root = new BTreeLeafNode<K, V>(this);
  }

  @Override
  public Object createNode(BTreeNode<K> node)
  {
    return node;
  }

  @Override
  public BTreeNode<K> getRoot()
  {
    return root;
  }

  @Override
  public void setRoot(BTreeNode<K> node)
  {
    this.root = node;
  }

  @SuppressWarnings("unchecked")
  @Override
  public BTreeNode<K> getChild(BTreeInnerNode<K> node, int index)
  {
    Object[] children = getChildren(node);
    return (BTreeNode<K>) children[index];
  }

  @Override
  public void setParent(BTreeNode<K> node, Object parentId)
  {
    node.parentNodeId = parentId;
  }

  @Override
  public void setChild(BTreeInnerNode<K> node, int index,
      BTreeNode<K> child)
  {
    Object[] children = getChildren(node);
    children[index] = child;
    if (child != null)
      child.setParent(this, node);
  }

  @Override
  public void setKey(BTreeNode<K> node, int index, K key)
  {
    node.keys[index] = key;
  }

  @Override
  public void setKeyCount(BTreeNode<K> node, int newKeyCount)
  {
    node.keyCount = newKeyCount;
  }

  @Override
  public void setValue(BTreeLeafNode<K, V> node, int index, V value)
  {
    node.values[index] = value;
  }

  @Override
  public void setLeftSibling(BTreeNode<K> node, Object siblingId)
  {
    node.leftSiblingId = siblingId;
  }

  @Override
  public void setRightSibling(BTreeNode<K> node, Object siblingId)
  {
    node.rightSiblingId = siblingId;
  }

  @SuppressWarnings("unchecked")
  @Override
  public BTreeNode<K> getNode(Object nodeId)
  {
    return (BTreeNode<K>) nodeId;
  }

  @Override
  public Tx<K> getTx()
  {
    return new Tx<K>()
    {
      @Override
      public void addNode(BTreeNode<K> node)
      {
      }

      @Override
      public void commit()
      {
      }
    };
  }

  private Object[] getChildren(BTreeInnerNode<K> node)
  {
    return node.children;
  }

}
