package cn.paxos.pocket.btree;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class BytesWrapper implements Comparable<BytesWrapper>, Iterable<Byte>
{
  
  private final List<byte[]> arrays;

  private int length;
  
  public BytesWrapper(byte[] bytes)
  {
    this.arrays = new ArrayList<byte[]>(1);
    this.arrays.add(bytes);
    this.length = bytes.length;
  }
  
  public BytesWrapper(int listSize)
  {
    this.arrays = new ArrayList<byte[]>(listSize);
    this.length = 0;
  }
  
  public BytesWrapper(List<byte[]> arrays, int length)
  {
    this.arrays = arrays;
    this.length = length;
  }
  
  public BytesWrapper()
  {
    this.arrays = new LinkedList<byte[]>();
    this.length = 0;
  }

  public void append(byte[] bytes)
  {
    arrays.add(bytes);
    length += bytes.length;
  }

  public void append(String str)
  {
    // TODO init once ?
    try
    {
      MessageDigest md = MessageDigest.getInstance("MD5");
      this.append(md.digest(str.getBytes()));
    } catch (NoSuchAlgorithmException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public int compareTo(BytesWrapper o)
  {
    int length1 = this.getLength();
    int length2 = o.getLength();
    if (this == o && length1 == length2)
    {
      return 0;
    }
    Iterator<Byte> iter1 = this.iterator();
    Iterator<Byte> iter2 = o.iterator();
    while (iter1.hasNext() && iter2.hasNext())
    {
      int a = (iter1.next() & 0xff);
      int b = (iter2.next() & 0xff);
      if (a != b)
      {
        return a - b;
      }
    }
    return length1 - length2;
  }

  @Override
  public Iterator<Byte> iterator()
  {
    return new Iterator<Byte>() {
      private final Iterator<byte[]> bytesIterator = arrays.iterator();
      private byte[] bytes = null;
      private int byteIndex = -1;
      {
        if (bytesIterator.hasNext())
        {
          bytes = bytesIterator.next();
          byteIndex = 0;
        }
      }
      @Override
      public boolean hasNext()
      {
        return bytes != null;
      }

      @Override
      public Byte next()
      {
        byte returned = bytes[byteIndex];
        byteIndex++;
        if (byteIndex >= bytes.length)
        {
          if (bytesIterator.hasNext())
          {
            bytes = bytesIterator.next();
            byteIndex = 0;
          } else
          {
            bytes = null;
            byteIndex = -1;
          }
        }
        return returned;
      }

      @Override
      public void remove()
      {
      }
      
    };
  }

  @SuppressWarnings("unchecked")
  public BytesWrapper increaseOne()
  {
    Deque<byte[]> deque1 = new LinkedList<byte[]>();
    for (byte[] bs : this.arrays)
    {
      deque1.offer(bs);
    }
    Deque<byte[]> deque2 = new LinkedList<byte[]>();
    boolean increased = false;
    while (!deque1.isEmpty())
    {
      byte[] bs = deque1.pollLast();
      if (!increased)
      {
        byte[] newBs = new byte[bs.length];
        System.arraycopy(bs, 0, newBs, 0, bs.length);
        bs:
          for (int i = bs.length - 1; i > -1; i--)
          {
            byte bb = bs[i];
            if ((((int) bb) & 0xFF) != 0xFF)
            {
              newBs[i] = (byte) (bb + 1);
              increased = true;
              break bs;
            } else
            {
              newBs[i] = 0;
            }
          }
        bs = newBs;
      }
      deque2.offerFirst(bs);
    }
    return new BytesWrapper((List<byte[]>) deque2, this.getLength());
  }

  public List<byte[]> getArrays()
  {
    return arrays;
  }

  public int getLength()
  {
    return length;
  }
  
  public static void main1(String[] args)
  {
    BytesWrapper bw = new BytesWrapper();
    bw.append(new byte[] { 0, 0 });
    bw.append(new byte[] { (byte) 0xff, (byte) 0xf0 });
    for (int i = 0; i < 100; i++)
    {
      bw = bw.increaseOne();
      String s = "";
      for (byte b : bw)
      {
        String ss = Integer.toHexString(b);
        s += ss.length() > 1 ? ss.substring(ss.length() - 2) : ("0" + ss);
      }
      System.out.println(s);
    }
  }
  
  public static void main(String[] args) throws NoSuchAlgorithmException
  {
    MessageDigest md = MessageDigest.getInstance("MD5");
    System.out.println(java.util.Arrays.toString(md.digest("abc".getBytes())));
    System.out.println(java.util.Arrays.toString(md.digest("abcd".getBytes())));
  }

}
