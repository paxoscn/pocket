package cn.paxos.pocket.btree;

import java.util.HashMap;
import java.util.Map;

public class Cache<K extends Comparable<K>>
{

  private final Map<Object, BTreeNode<K>> map = new HashMap<Object, BTreeNode<K>>();

  public BTreeNode<K> get(Object nodeId)
  {
    return map.get(nodeId);
  }

  public void put(Object nodeId, BTreeNode<K> node)
  {
    map.put(nodeId, node);
  }

}
