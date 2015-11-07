package cn.paxos.pocket;

import cn.paxos.pocket.btree.BytesWrapper;

public class PrintAll
{
  
  public static void main(String[] args)
  {
    Pocket pocket = new Pocket(args[0]);
    BytesWrapper key = new BytesWrapper();
    Iterable<Gadget> list = pocket.scan(key);
    for (Gadget row : list)
    {
      System.out.println(row.getAttribute("url"));
    }
  }

}
