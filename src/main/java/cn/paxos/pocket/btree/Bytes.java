package cn.paxos.pocket.btree;

import java.util.Arrays;

public class Bytes implements Comparable<Bytes>
{

  private final byte[] bytes;

  public Bytes(byte[] bytes)
  {
    this.bytes = bytes;
  }

  @Override
  public int compareTo(Bytes o)
  {
    byte[] buffer1 = this.bytes;
    byte[] buffer2 = o.bytes;
    int offset1 = 0;
    int offset2 = 0;
    int length1 = buffer1.length;
    int length2 = buffer2.length;
    // Short circuit equal case
    if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2)
    {
      return 0;
    }
    // Bring WritableComparator code local
    int end1 = offset1 + length1;
    int end2 = offset2 + length2;
    for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++)
    {
      int a = (buffer1[i] & 0xff);
      int b = (buffer2[j] & 0xff);
      if (a != b)
      {
        return a - b;
      }
    }
    return length1 - length2;
  }

  @Override
  public int hashCode()
  {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(bytes);
    return result;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Bytes other = (Bytes) obj;
    if (!Arrays.equals(bytes, other.bytes))
      return false;
    return true;
  }

  public byte[] getBytes()
  {
    return bytes;
  }

}