package cn.paxos.pocket.btree;

import java.util.Iterator;

public class BTree<K extends Comparable<K>, V>
{

  private final Disk<K, V> disk;

  public BTree(Disk<K, V> disk)
  {
    this.disk = disk;
  }

  public void insert(K key, V value)
  {
    Tx<K> tx = disk.getTx();
    BTreeLeafNode<K, V> leaf = this.findLeafNodeShouldContainKey(key);
    leaf.insertKey(key, value);

    if (leaf.isOverflow())
    {
      BTreeNode<K> n = leaf.dealOverflow(disk);
      if (n != null)
        disk.setRoot(n);
    }
    tx.commit();
  }

  public V search(K key)
  {
    BTreeLeafNode<K, V> leaf = this.findLeafNodeShouldContainKey(key);

    int index = leaf.search(key);
    return (index == -1) ? null : leaf.getValue(index);
  }

  @SuppressWarnings("unchecked")
  public Iterable<V> search(final K start, final K stop)
  {
    return new Iterable<V>() {
      @Override
      public Iterator<V> iterator()
      {
        return new Iterator<V>() {
          private BTreeLeafNode<K, V> leaf = null;
          private V value = null;
          private int index = -1;
          {
            leaf = findLeafNodeShouldContainKey(start);
            index = leaf.ge(start);
            if (index != -1)
            {
              int cmp = leaf.getKey(index).compareTo(stop);
              if (cmp < 0)
              {
                value = leaf.getValue(index);
                index++;
                if (index >= leaf.getKeyCount())
                {
                  leaf = (BTreeLeafNode<K, V>) leaf.getRightSibling(disk);
                  index = 0;
                }
              }
            }
          }
          @Override
          public boolean hasNext()
          {
            return value != null;
          }
          @Override
          public V next()
          {
            V returned = value;
            if (leaf == null)
            {
              value = null;
              return returned;
            }
            int cmp = leaf.getKey(index).compareTo(stop);
            if (cmp >= 0)
            {
              value = null;
              return returned;
            }
            value = leaf.getValue(index);
            index++;
            if (index >= leaf.getKeyCount())
            {
              leaf = (BTreeLeafNode<K, V>) leaf.getRightSibling(disk);
              index = 0;
            }
            return returned;
          }
          @Override
          public void remove()
          {
          }
        };
      }
    };
  }

  public void delete(K key)
  {
    BTreeLeafNode<K, V> leaf = this.findLeafNodeShouldContainKey(key);

    if (leaf.delete(key) && leaf.isUnderflow())
    {
      BTreeNode<K> n = leaf.dealUnderflow(disk);
      if (n != null)
        disk.setRoot(n);
    }
  }

  @SuppressWarnings("unchecked")
  private BTreeLeafNode<K, V> findLeafNodeShouldContainKey(K key)
  {
    BTreeNode<K> node = disk.getRoot();
    while (node.getNodeType() == TreeNodeType.InnerNode)
    {
      node = ((BTreeInnerNode<K>) node).getChild(node.search(key));
    }

    return (BTreeLeafNode<K, V>) node;
  }
}
