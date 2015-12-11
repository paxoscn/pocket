package cn.paxos.pocket.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class BytesUtils
{

  public static byte[] intToBytes(int value)
  {
    byte[] bytes = new byte[4];
    for (int i = bytes.length - 1; i >= 0; i--)
    {
      int offset = i * 8;
      bytes[i] = (byte) (value >> offset);
    }
    return bytes;
  }

  public static byte[] longToBytes(long value)
  {
    byte[] bytes = new byte[8];
    for (int i = bytes.length - 1; i >= 0; i--)
    {
      int offset = i * 8;
      bytes[i] = (byte) (value >> offset);
    }
    return bytes;
  }

  public static byte[] longToBytesReversed(long value)
  {
    byte[] bytes = new byte[8];
    for (int i = bytes.length - 1; i >= 0; i--)
    {
      int offset = i * 8;
      bytes[7 - i] = (byte) (value >> offset);
    }
    return bytes;
  }

  public static int bytesToInt(byte[] bytes)
  {
    return bytesToInt(bytes, 0);
  }

  public static int bytesToInt(byte[] bytes, int bytesOffset)
  {
    if (bytes.length < 4)
    {
      System.out.println("Out 3");
      System.out.println(java.util.Arrays.toString(bytes));
      System.out.println(bytesOffset);
      new RuntimeException().printStackTrace();
      System.exit(-1);
    }
    int intValue = 0;
    for (int i = 4 - 1; i >= 0; i--)
    {
      int offset = i * 8;
      intValue |= (bytes[bytesOffset + i] & 0xFF) << offset;
    }
    return intValue;
  }
  
  public static byte[] md5(String str)
  {
    // TODO init once ?
    try
    {
      MessageDigest md = MessageDigest.getInstance("MD5");
      return md.digest(str.getBytes());
    } catch (NoSuchAlgorithmException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

}
