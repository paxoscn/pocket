package cn.paxos.pocket;

public class PrintAll
{
  
  public static void main(String args[])
  {
    Pocket pocket = new Pocket(args[0]);
    for(Gadget row : pocket.scan())
    {
      System.out.println(row.getAttribute("url"));
    }
  }

}
