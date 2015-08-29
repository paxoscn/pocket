package cn.paxos.pocket;

import cn.paxos.pocket.btree.BytesWrapper;

public class PocketTest
{
  
  public static void main(String[] args)
  {
    Pocket pocket = new Pocket("/tmp/pocket/fav");
    addFav(pocket, "user1", "www.apple.com", 3);
    addFav(pocket, "user1", "www.bbc.com", 2);
    addFav(pocket, "user2", "www.twitter.com", 5);
    addFav(pocket, "user2", "www.facebook.com", 1);
    addFav(pocket, "user1", "www.bbc.com", 1);
    System.out.println(1 == getScore(pocket, "user1", "www.bbc.com"));
    System.out.println(0 == getScore(pocket, "user2", "www.bbc.com"));
    for (Gadget row : listFav(pocket, "user1"))
    {
      System.out.println(row.getAttribute("url") + " " + row.getAttribute("score"));
    }
  }

  private static void addFav(Pocket pocket, String user, String url, int score)
  {
    BytesWrapper key = new BytesWrapper();
    key.append(user);
    key.append(url);
    Gadget row = new Gadget(key, true);
    row.setAttribute("url", url);
    row.setAttribute("score", Integer.toString(score));
    pocket.put(row);
  }

  private static int getScore(Pocket pocket, String user, String url)
  {
    BytesWrapper key = new BytesWrapper();
    key.append(user);
    key.append(url);
    Gadget row = pocket.get(key);
    if (row == null)
    {
      return 0;
    }
    return Integer.parseInt(row.getAttribute("score"));
  }

  private static Iterable<Gadget> listFav(Pocket pocket, String user)
  {
    BytesWrapper key = new BytesWrapper();
    key.append(user);
    return pocket.scan(key);
  }

}
