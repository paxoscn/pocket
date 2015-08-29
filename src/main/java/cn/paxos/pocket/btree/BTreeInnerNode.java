package cn.paxos.pocket.btree;

class BTreeInnerNode<K extends Comparable<K>> extends BTreeNode<K>
{

  private final Disk<K, ?> disk;

  private final Object id;

  Object[] children;

  public BTreeInnerNode(Disk<K, ?> disk)
  {
    this.disk = disk;
    this.id = disk.createNode(this);
    this.children = new Object[ORDER + 2];
  }

  public BTreeInnerNode(Disk<K, ?> disk, Object id)
  {
    this.disk = disk;
    this.id = id;
    this.children = new Object[ORDER + 2];
  }

  public Object getId()
  {
    return id;
  }

  public BTreeNode<K> getChild(int index)
  {
    return (BTreeNode<K>) disk.getChild(this, index);
  }

  public void setChild(int index, BTreeNode<K> child)
  {
    disk.setChild(this, index, child);
  }

  @Override
  public TreeNodeType getNodeType()
  {
    return TreeNodeType.InnerNode;
  }

  @Override
  public int search(K key)
  {
    int index = 0;
    for (index = 0; index < this.getKeyCount(); ++index)
    {
      int cmp = this.getKey(index).compareTo(key);
      if (cmp == 0)
      {
        return index + 1;
      } else if (cmp > 0)
      {
        return index;
      }
    }

    return index;
  }

  /* The codes below are used to support insertion operation */

  private void insertAt(int index, K key, BTreeNode<K> leftChild,
      BTreeNode<K> rightChild)
  {
    // move space for the new key
    for (int i = this.getKeyCount() + 1; i > index; --i)
    {
      this.setChild(i, this.getChild(i - 1));
    }
    for (int i = this.getKeyCount(); i > index; --i)
    {
      this.setKey(disk, i, this.getKey(i - 1));
    }

    // insert the new key
    this.setKey(disk, index, key);
    this.setChild(index, leftChild);
    this.setChild(index + 1, rightChild);
    disk.setKeyCount(this, this.getKeyCount() + 1);
  }

  /**
   * When splits a internal node, the middle key is kicked out and be pushed to
   * parent node.
   */
  @Override
  protected BTreeNode<K> split()
  {
    int midIndex = this.getKeyCount() / 2;

    BTreeInnerNode<K> newRNode = new BTreeInnerNode<K>(disk);
    for (int i = midIndex + 1; i < this.getKeyCount(); ++i)
    {
      newRNode.setKey(disk, i - midIndex - 1, this.getKey(i));
      this.setKey(disk, i, null);
    }
    for (int i = midIndex + 1; i <= this.getKeyCount(); ++i)
    {
      newRNode.setChild(i - midIndex - 1, this.getChild(i));
      newRNode.getChild(i - midIndex - 1).setParent(disk, newRNode);
      this.setChild(i, null);
    }
    this.setKey(disk, midIndex, null);
    disk.setKeyCount(newRNode, this.getKeyCount() - midIndex - 1);
    disk.setKeyCount(this, midIndex);

    return newRNode;
  }

  @Override
  protected BTreeNode<K> pushUpKey(K key, BTreeNode<K> leftChild,
      BTreeNode<K> rightNode)
  {
    // find the target position of the new key
    int index = this.search(key);

    // insert the new key
    this.insertAt(index, key, leftChild, rightNode);

    // check whether current node need to be split
    if (this.isOverflow())
    {
      return this.dealOverflow(disk);
    } else
    {
      return this.getParentId() == null ? this : null;
    }
  }

  /* The codes below are used to support delete operation */

  private void deleteAt(int index)
  {
    int i = 0;
    for (i = index; i < this.getKeyCount() - 1; ++i)
    {
      this.setKey(disk, i, this.getKey(i + 1));
      this.setChild(i + 1, this.getChild(i + 2));
    }
    this.setKey(disk, i, null);
    this.setChild(i + 1, null);
    disk.setKeyCount(this, this.getKeyCount() - 1);
  }

  @Override
  protected void processChildrenTransfer(BTreeNode<K> borrower,
      BTreeNode<K> lender, int borrowIndex)
  {
    int borrowerChildIndex = 0;
    while (borrowerChildIndex < this.getKeyCount() + 1
        && this.getChild(borrowerChildIndex) != borrower)
      ++borrowerChildIndex;

    if (borrowIndex == 0)
    {
      // borrow a key from right sibling
      K upKey = borrower.transferFromSibling(
          this.getKey(borrowerChildIndex), lender, borrowIndex);
      this.setKey(disk, borrowerChildIndex, upKey);
    } else
    {
      // borrow a key from left sibling
      K upKey = borrower.transferFromSibling(
          this.getKey(borrowerChildIndex - 1), lender, borrowIndex);
      this.setKey(disk, borrowerChildIndex - 1, upKey);
    }
  }

  @Override
  protected BTreeNode<K> processChildrenFusion(BTreeNode<K> leftChild,
      BTreeNode<K> rightChild)
  {
    int index = 0;
    while (index < this.getKeyCount() && this.getChild(index) != leftChild)
      ++index;
    K sinkKey = this.getKey(index);

    // merge two children and the sink key into the left child node
    leftChild.fusionWithSibling(sinkKey, rightChild);

    // remove the sink key, keep the left child and abandon the right child
    this.deleteAt(index);

    // check whether need to propagate borrow or fusion to parent
    if (this.isUnderflow())
    {
      if (this.getParentId() == null)
      {
        // current node is root, only remove keys or delete the whole root node
        if (this.getKeyCount() == 0)
        {
          leftChild.setParent(disk, (Object) null);
          return leftChild;
        } else
        {
          return null;
        }
      }

      return this.dealUnderflow(disk);
    }

    return null;
  }

  @Override
  protected void fusionWithSibling(K sinkKey, BTreeNode<K> rightSibling)
  {
    BTreeInnerNode<K> rightSiblingNode = (BTreeInnerNode<K>) rightSibling;

    int j = this.getKeyCount();
    this.setKey(disk, j++, sinkKey);

    for (int i = 0; i < rightSiblingNode.getKeyCount(); ++i)
    {
      this.setKey(disk, j + i, rightSiblingNode.getKey(i));
    }
    for (int i = 0; i < rightSiblingNode.getKeyCount() + 1; ++i)
    {
      this.setChild(j + i, rightSiblingNode.getChild(i));
    }
    disk.setKeyCount(this,
        this.getKeyCount() + 1 + rightSiblingNode.getKeyCount());

    this.setRightSibling(disk, rightSiblingNode.getRightSiblingId());
    if (rightSiblingNode.getRightSiblingId() != null)
      rightSiblingNode.getRawRightSibling(disk).setLeftSibling(disk, this);
  }

  @Override
  protected K transferFromSibling(K sinkKey, BTreeNode<K> sibling,
      int borrowIndex)
  {
    BTreeInnerNode<K> siblingNode = (BTreeInnerNode<K>) sibling;

    K upKey = null;
    if (borrowIndex == 0)
    {
      // borrow the first key from right sibling, append it to tail
      int index = this.getKeyCount();
      this.setKey(disk, index, sinkKey);
      this.setChild(index + 1, siblingNode.getChild(borrowIndex));
      disk.setKeyCount(this, this.getKeyCount() + 1);

      upKey = siblingNode.getKey(0);
      siblingNode.deleteAt(borrowIndex);
    } else
    {
      // borrow the last key from left sibling, insert it to head
      this.insertAt(0, sinkKey, siblingNode.getChild(borrowIndex + 1),
          this.getChild(0));
      upKey = siblingNode.getKey(borrowIndex);
      siblingNode.deleteAt(borrowIndex);
    }

    return upKey;
  }
}