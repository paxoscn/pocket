package cn.paxos.pocket;

import java.util.Iterator;
import java.util.Map;

import cn.paxos.pocket.btree.BTree;
import cn.paxos.pocket.btree.BytesWrapper;
import cn.paxos.pocket.btree.DefaultDisk;

/**
 * Doraemon's Forth-Dimensional Pocket
 */
public class Pocket
{
  
  private final BTree<BytesWrapper, BytesWrapper> tree;
  
  public Pocket(String folder)
  {
    this.tree = new BTree<BytesWrapper, BytesWrapper>(new DefaultDisk(folder));
  }

  public Gadget get(BytesWrapper key)
  {
    BytesWrapper value = tree.search(key);
    if (value == null)
    {
      return null;
    }
    return new Gadget(value, false);
  }
  
  public Iterable<Gadget> scan(final BytesWrapper key)
  {
    return new Iterable<Gadget>() {
      @Override
      public Iterator<Gadget> iterator()
      {
        return new Iterator<Gadget>() {
          final Iterator<BytesWrapper> iter;
          {
            BytesWrapper start = key;
            BytesWrapper stop = start.increaseOne();
            iter = tree.search(start, stop).iterator();
          }
          @Override
          public boolean hasNext()
          {
            return iter.hasNext();
          }
          @Override
          public Gadget next()
          {
            BytesWrapper value = iter.next();
            return new Gadget(value, false);
          }
          @Override
          public void remove()
          {
          }
        };
      }
    };
  }
  
  public Iterable<Gadget> search(String key, Map<String, String> attributeKeywords)
  {
    return null;
  }
  
  public void put(Gadget gadget)
  {
    gadget.prepareBinary();
    tree.insert(gadget.getKey(), gadget.getBinary());
  }

}
