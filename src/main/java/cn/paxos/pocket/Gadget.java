package cn.paxos.pocket;

import static cn.paxos.pocket.util.BytesUtils.bytesToInt;
import static cn.paxos.pocket.util.BytesUtils.intToBytes;

import java.nio.charset.Charset;
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

}
