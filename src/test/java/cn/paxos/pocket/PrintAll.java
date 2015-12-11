package cn.paxos.pocket;

import java.util.Iterator;

public class PrintAll
{
  
  public static void main(String args[])
  {
    Pocket pocket = new Pocket(args[0]);
    for(Gadget row : pocket.scan())
    {
      for (Iterator<String> iterator = row.iterateAttributeNames(); iterator.hasNext();)
      {
        String attributeName = iterator.next();
        System.out.println(attributeName + " = " + row.getAttribute(attributeName));
      }
    }
  }

}
