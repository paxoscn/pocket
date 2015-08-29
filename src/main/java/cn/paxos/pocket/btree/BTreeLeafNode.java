package cn.paxos.pocket.btree;

class BTreeLeafNode<K extends Comparable<K>, V> extends
    BTreeNode<K>
{

  private final Disk<K, V> disk;

  private final Object id;

  Object[] values;

  public BTreeLeafNode(Disk<K, V> disk)
  {
    this.disk = disk;
    this.values = new Object[this.getKeyNum()];
    this.id = disk.createNode(this);
  }

  public BTreeLeafNode(Disk<K, V> disk, Object id)
  {
    this.disk = disk;
    this.values = new Object[this.getKeyNum()];
    this.id = id;
  }

  public Object getId()
  {
    return id;
  }

  @SuppressWarnings("unchecked")
  public V getValue(int index)
  {
    return (V) this.values[index];
  }

  public void setValue(int index, V value)
  {
    disk.setValue(this, index, value);
  }

  @Override
  public TreeNodeType getNodeType()
  {
    return TreeNodeType.LeafNode;
  }

  @Override
  public int search(K key)
  {
    for (int i = 0; i < this.getKeyCount(); ++i)
    {
      int cmp = this.getKey(i).compareTo(key);
      if (cmp == 0)
      {
        return i;
      } else if (cmp > 0)
      {
        return -1;
      }
    }

    return -1;
  }

  // Mergen
  public int ge(K key)
  {
    for (int i = 0; i < this.getKeyCount(); ++i)
    {
      int cmp = this.getKey(i).compareTo(key);
      if (cmp >= 0)
      {
        return i;
      }
    }

    return -1;
  }

  /* The codes below are used to support insertion operation */

  public void insertKey(K key, V value)
  {
    int index = 0;
    while (index < this.getKeyCount() && this.getKey(index).compareTo(key) < 0)
      ++index;
    K existing = this.getKey(index);
    if (existing != null && existing.compareTo(key) == 0)
    {
      this.replaceAt(index, key, value);
    } else
    {
      this.insertAt(index, key, value);
    }
  }

  private void insertAt(int index, K key, V value)
  {
    // move space for the new key
    for (int i = this.getKeyCount() - 1; i >= index; --i)
    {
      this.setKey(disk, i + 1, this.getKey(i));
      this.setValue(i + 1, this.getValue(i));
    }

    // insert new key and value
    this.setKey(disk, index, key);
    this.setValue(index, value);
    disk.setKeyCount(this, this.getKeyCount() + 1);
  }

  private void replaceAt(int index, K key, V value)
  {
    this.setValue(index, value);
  }

  /**
   * When splits a leaf node, the middle key is kept on new node and be pushed
   * to parent node.
   */
  @Override
  protected BTreeNode<K> split()
  {
    int midIndex = this.getKeyCount() / 2;

    BTreeLeafNode<K, V> newRNode = new BTreeLeafNode<K, V>(disk);
    for (int i = midIndex; i < this.getKeyCount(); ++i)
    {
      newRNode.setKey(disk, i - midIndex, this.getKey(i));
      newRNode.setValue(i - midIndex, this.getValue(i));
      this.setKey(disk, i, null);
      this.setValue(i, null);
    }
    disk.setKeyCount(newRNode, this.getKeyCount() - midIndex);
    disk.setKeyCount(this, midIndex);

    return newRNode;
  }

  @Override
  protected BTreeNode<K> pushUpKey(K key, BTreeNode<K> leftChild,
      BTreeNode<K> rightNode)
  {
    throw new UnsupportedOperationException();
  }

  /* The codes below are used to support deletion operation */

  public boolean delete(K key)
  {
    int index = this.search(key);
    if (index == -1)
      return false;

    this.deleteAt(index);
    return true;
  }

  private void deleteAt(int index)
  {
    int i = index;
    for (i = index; i < this.getKeyCount() - 1; ++i)
    {
      this.setKey(disk, i, this.getKey(i + 1));
      this.setValue(i, this.getValue(i + 1));
    }
    this.setKey(disk, i, null);
    this.setValue(i, null);
    disk.setKeyCount(this, this.getKeyCount() - 1);
  }

  @Override
  protected void processChildrenTransfer(BTreeNode<K> borrower,
      BTreeNode<K> lender, int borrowIndex)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  protected BTreeNode<K> processChildrenFusion(BTreeNode<K> leftChild,
      BTreeNode<K> rightChild)
  {
    throw new UnsupportedOperationException();
  }

  /**
   * Notice that the key sunk from parent is be abandoned.
   */
  @Override
  @SuppressWarnings("unchecked")
  protected void fusionWithSibling(K sinkKey, BTreeNode<K> rightSibling)
  {
    BTreeLeafNode<K, V> siblingLeaf = (BTreeLeafNode<K, V>) rightSibling;

    int j = this.getKeyCount();
    for (int i = 0; i < siblingLeaf.getKeyCount(); ++i)
    {
      this.setKey(disk, j + i, siblingLeaf.getKey(i));
      this.setValue(j + i, siblingLeaf.getValue(i));
    }
    disk.setKeyCount(this, this.getKeyCount() + siblingLeaf.getKeyCount());

    this.setRightSibling(disk, siblingLeaf.getRightSiblingId());
    if (siblingLeaf.getRawRightSibling(disk) != null)
      siblingLeaf.getRawRightSibling(disk).setLeftSibling(disk, this);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected K transferFromSibling(K sinkKey, BTreeNode<K> sibling,
      int borrowIndex)
  {
    BTreeLeafNode<K, V> siblingNode = (BTreeLeafNode<K, V>) sibling;

    this.insertKey(siblingNode.getKey(borrowIndex),
        siblingNode.getValue(borrowIndex));
    siblingNode.deleteAt(borrowIndex);

    return borrowIndex == 0 ? sibling.getKey(0) : this.getKey(0);
  }
}
