package cn.paxos.pocket.btree;

enum TreeNodeType
{
  InnerNode, LeafNode
}

abstract class BTreeNode<K extends Comparable<K>>
{

  final static int ORDER = 128;

  Object[] keys;
  int keyCount;
  Object parentNodeId;
  Object leftSiblingId;
  Object rightSiblingId;

  protected BTreeNode()
  {
    this.keys = new Object[ORDER + 1];
    this.keyCount = 0;
    this.parentNodeId = null;
    this.leftSiblingId = null;
    this.rightSiblingId = null;
  }

  public int getKeyCount()
  {
    return this.keyCount;
  }

  @SuppressWarnings("unchecked")
  public K getKey(int index)
  {
    return (K) this.keys[index];
  }

  public void setKey(Disk<K, ?> disk, int index, K key)
  {
    disk.setKey(this, index, key);
  }

  public BTreeNode<K> getParent(Disk<K, ?> disk)
  {
    return disk.getNode(this.parentNodeId);
  }

  public Object getParentId()
  {
    return this.parentNodeId;
  }

  public void setParent(Disk<K, ?> disk, BTreeNode<K> parent)
  {
    disk.setParent(this, parent.getId());
  }

  public void setParent(Disk<K, ?> disk, Object parentId)
  {
    disk.setParent(this, parentId);
  }

  public abstract TreeNodeType getNodeType();

  /**
   * Search a key on current node, if found the key then return its position,
   * otherwise return -1 for a leaf node, return the child node index which
   * should contain the key for a internal node.
   */
  public abstract int search(K key);

  /* The codes below are used to support insertion operation */

  public boolean isOverflow()
  {
    return this.getKeyCount() == this.keys.length;
  }

  public BTreeNode<K> dealOverflow(Disk<K, ?> disk)
  {
    int midIndex = this.getKeyCount() / 2;
    K upKey = this.getKey(midIndex);

    BTreeNode<K> newRNode = this.split();

    if (this.getParentId() == null)
    {
      this.setParent(disk, new BTreeInnerNode<K>(disk));
    }
    newRNode.setParent(disk, this.getParentId());

    // maintain links of sibling nodes
    newRNode.setLeftSibling(disk, this);
    newRNode.setRightSibling(disk, this.rightSiblingId);
    if (this.getRightSibling(disk) != null)
      this.getRightSibling(disk).setLeftSibling(disk, newRNode);
    this.setRightSibling(disk, newRNode.getId());

    // push up a key to parent internal node
    return this.getParent(disk).pushUpKey(upKey, this, newRNode);
  }

  protected abstract BTreeNode<K> split();

  protected abstract BTreeNode<K> pushUpKey(K key,
      BTreeNode<K> leftChild, BTreeNode<K> rightNode);

  protected abstract Object getId();

  /* The codes below are used to support deletion operation */

  public boolean isUnderflow()
  {
    return this.getKeyCount() < (this.keys.length / 2);
  }

  public boolean canLendAKey()
  {
    return this.getKeyCount() > (this.keys.length / 2);
  }

  public BTreeNode<K> getLeftSibling(Disk<K, ?> disk)
  {
    BTreeNode<K> leftSibling;
    if (this.leftSiblingId != null)
    {
      leftSibling = disk.getNode(this.leftSiblingId);
      if (leftSibling.getParent(disk) == this.getParent(disk))
        return leftSibling;
    }
    return null;
  }

  public void setLeftSibling(Disk<K, ?> disk, BTreeNode<K> sibling)
  {
    disk.setLeftSibling(this, sibling.getId());
  }

  public BTreeNode<K> getRightSibling(Disk<K, ?> disk)
  {
    BTreeNode<K> rightSibling;
    if (this.rightSiblingId != null)
    {
      rightSibling = disk.getNode(this.rightSiblingId);
      if (rightSibling.getParent(disk) == this.getParent(disk))
        return rightSibling;
    }
    return null;
  }

  public void setRightSibling(Disk<K, ?> disk, Object siblingId)
  {
    disk.setRightSibling(this, siblingId);
  }

  public BTreeNode<K> dealUnderflow(Disk<K, ?> disk)
  {
    if (this.getParent(disk) == null)
      return null;

    // try to borrow a key from sibling
    BTreeNode<K> leftSibling = this.getLeftSibling(disk);
    if (leftSibling != null && leftSibling.canLendAKey())
    {
      this.getParent(disk).processChildrenTransfer(this, leftSibling,
          leftSibling.getKeyCount() - 1);
      return null;
    }

    BTreeNode<K> rightSibling = this.getRightSibling(disk);
    if (rightSibling != null && rightSibling.canLendAKey())
    {
      this.getParent(disk).processChildrenTransfer(this, rightSibling, 0);
      return null;
    }

    // Can not borrow a key from any sibling, then do fusion with sibling
    if (leftSibling != null)
    {
      return this.getParent(disk).processChildrenFusion(leftSibling, this);
    } else
    {
      return this.getParent(disk).processChildrenFusion(this, rightSibling);
    }
  }

  protected final int getKeyNum()
  {
    return keys.length;
  }

  protected BTreeNode<K> getRawRightSibling(Disk<K, ?> disk)
  {
    return this.rightSiblingId == null ? null : disk
        .getNode(this.rightSiblingId);
  }

  protected Object getRightSiblingId()
  {
    return this.rightSiblingId;
  }

  protected abstract void processChildrenTransfer(BTreeNode<K> borrower,
      BTreeNode<K> lender, int borrowIndex);

  protected abstract BTreeNode<K> processChildrenFusion(
      BTreeNode<K> leftChild, BTreeNode<K> rightChild);

  protected abstract void fusionWithSibling(K sinkKey,
      BTreeNode<K> rightSibling);

  protected abstract K transferFromSibling(K sinkKey,
      BTreeNode<K> sibling, int borrowIndex);
}