package cn.paxos.pocket.util;

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

  public static int bytesToInt(byte[] bytes, int bytesOffset)
  {
    int intValue = 0;
    for (int i = 4 - 1; i >= 0; i--)
    {
      int offset = i * 8;
      intValue |= (bytes[bytesOffset + i] & 0xFF) << offset;
    }
    return intValue;
  }

}
