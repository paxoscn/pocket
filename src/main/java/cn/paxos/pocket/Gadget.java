package cn.paxos.pocket;

import static cn.paxos.pocket.util.BytesUtils.bytesToInt;
import static cn.paxos.pocket.util.BytesUtils.intToBytes;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cn.paxos.pocket.btree.BytesWrapper;

public class Gadget
{

  private static final String BLANK_STRING = "";
  private static final Charset ISO = Charset.forName("ISO-8859-1");
  private static final Charset UTF8 = Charset.forName("UTF-8");
  
  private final BytesWrapper key;
  private final Map<String, String> attributes;
  
  private BytesWrapper binary;
  
  public Gadget(BytesWrapper bytes, boolean isKey)
  {
    if (isKey)
    {
      if (bytes == null)
      {
        // TODO
        throw new RuntimeException("The key must not be null");
      }
      this.key = bytes;
      this.attributes = new HashMap<String, String>();
      this.binary = null;
    } else
    {
      Iterator<Byte> valueIter = bytes.iterator();
      int keyLength = bytesToInt(iter(valueIter, 4), 0);
      final byte[] keyBytes = iter(valueIter, keyLength);
      this.key = new BytesWrapper(keyBytes);
      this.attributes = new HashMap<String, String>();
      this.binary = bytes;
      while (valueIter.hasNext())
      {
        int attrNameLength = bytesToInt(iter(valueIter, 4), 0);
        final byte[] attrNameBytes = iter(valueIter, attrNameLength);
        int attrValueLength = bytesToInt(iter(valueIter, 4), 0);
        final byte[] attrValueBytes = iter(valueIter, attrValueLength);
        attributes.put(new String(attrNameBytes), new String(attrValueBytes));
      }
    }
  }
  private Gadget(byte[] newKeyPrefix, BytesWrapper oldKey, Map<String, String> oldAttributes, BytesWrapper oldBinary)
  {
    this.key = cloneAndReplace(0, oldKey, newKeyPrefix);
    this.attributes = oldAttributes;
    if (oldBinary != null)
    {
      // java.lang.IndexOutOfBoundsException: Index: 1, Size: 1
      if (oldBinary.getArrays().size() < 2)
      {
        List<byte[]> oldArrays = oldBinary.getArrays();
        List<byte[]> newArrays = new ArrayList<byte[]>(oldArrays);
        byte[] array = newArrays.get(0);
        byte[] newArray = new byte[array.length];
        System.arraycopy(array, 0, newArray, 0, 4);
        System.arraycopy(newKeyPrefix, 0, newArray, 4, newKeyPrefix.length);
        if (newArray.length - newKeyPrefix.length - 4 > 0)
        {
          System.arraycopy(array, 4 + newKeyPrefix.length, newArray, 4 + newKeyPrefix.length, newArray.length - newKeyPrefix.length - 4);
        }
        newArrays.set(0, newArray);
        this.binary = new BytesWrapper(newArrays, oldBinary.getLength());
      } else
      {
        this.binary = cloneAndReplace(1, oldBinary, newKeyPrefix);
      }
    }
    
  }
  public BytesWrapper getKey()
  {
    return key;
  }
  public String getAttribute(String attributeName)
  {
    return attributes.get(attributeName);
  }
  public void setAttribute(String attributeName, String attributeValue)
  {
    if (attributeName == null)
    {
      return;
    }
    if (attributeValue == null)
    {
      attributeValue = BLANK_STRING;
    }
    attributes.put(attributeName, attributeValue);
  }
  public Iterator<String> iterateAttributeNames()
  {
    return attributes.keySet().iterator();
  }
  public Gadget clone(byte[] newKeyPrefix)
  {
    return new Gadget(newKeyPrefix, this.key, this.attributes, this.binary);
  }
  BytesWrapper getBinary()
  {
    if (binary == null)
    {
      prepareBinary();
    }
    return binary;
  }
  void prepareBinary()
  {
    binary = new BytesWrapper(2 + 4 * attributes.size());
    List<byte[]> keyArrays = key.getArrays();
    binary.append(intToBytes(key.getLength()));
    for (byte[] bs : keyArrays)
    {
      binary.append(bs);
    }
    for (String attributeName : attributes.keySet())
    {
      String attributeValue = attributes.get(attributeName);
      byte[] attributeNameBytes = attributeName.getBytes(ISO);
      byte[] attributeValueBytes = attributeValue.getBytes(UTF8);
      binary.append(intToBytes(attributeNameBytes.length));
      binary.append(attributeNameBytes);
      binary.append(intToBytes(attributeValueBytes.length));
      binary.append(attributeValueBytes);
    }
  }
  private byte[] iter(Iterator<Byte> iter, int length)
  {
    byte[] bs = new byte[length];
    for (int i = 0; i < length; i++)
    {
      bs[i] = iter.next();
    }
    return bs;
  }
  private BytesWrapper cloneAndReplace(int arrayOffset, BytesWrapper old, byte[] replacement)
  {
    List<byte[]> oldArrays = old.getArrays();
    List<byte[]> newArrays = new ArrayList<byte[]>(oldArrays);
    byte[] array = newArrays.get(arrayOffset);
    byte[] newArray = new byte[array.length];
    System.arraycopy(replacement, 0, newArray, 0, replacement.length);
    if (newArray.length - replacement.length > 0)
    {
      System.arraycopy(array, replacement.length, newArray, replacement.length, newArray.length - replacement.length);
    }
    newArrays.set(arrayOffset, newArray);
    return new BytesWrapper(newArrays, old.getLength());
  }

}
