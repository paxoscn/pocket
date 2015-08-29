package cn.paxos.pocket.btree;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static cn.paxos.pocket.util.BytesUtils.bytesToInt;
import static cn.paxos.pocket.util.BytesUtils.intToBytes;
import static cn.paxos.pocket.util.BytesUtils.longToBytes;

public class DefaultDisk implements
    Disk<BytesWrapper, BytesWrapper>
{

  private static final int MAX_CHILDREN = 256;

  private final String baseFolderPath;

  private final Cache<BytesWrapper> cache;

  private Tx<BytesWrapper> tx = null;

  public DefaultDisk(String baseFolderPath)
  {
    this.baseFolderPath = baseFolderPath;
    File folder = new File(baseFolderPath + "/data");
    if (!folder.exists())
    {
      folder.mkdirs();
    }
    this.cache = new Cache<BytesWrapper>();
  }

  @Override
  public Object createNode(BTreeNode<BytesWrapper> node)
  {
    File newFile = null;
    long mostSignificantBits = -1;
    long leastSignificantBits = -1;
    try
    {
      boolean successful = false;
      int tried = 0;
      do
      {
        if (tried > 2)
        {
          throw new IOException("Failed to create");
        }
        UUID uuid = UUID.randomUUID();
        mostSignificantBits = uuid.getMostSignificantBits();
        leastSignificantBits = uuid.getLeastSignificantBits();
        int[] parts = new int[]
        { (int) ((mostSignificantBits & 0xffffffffL) % MAX_CHILDREN),
            (int) ((mostSignificantBits >>> 32) % MAX_CHILDREN),
            (int) ((leastSignificantBits & 0xffffffffL) % MAX_CHILDREN),
            (int) ((leastSignificantBits >>> 32) % MAX_CHILDREN) };
        File dataFolder = new File(baseFolderPath + "/data"
            + partsToFolder(parts));
        dataFolder.mkdirs();
        newFile = new File(dataFolder, Integer.toString(parts[3]));
        successful = newFile.createNewFile();
        tried++;
      } while (!successful);
    } catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    byte[] id = new byte[16];
    System.arraycopy(longToBytes(mostSignificantBits), 0, id, 0, 8);
    System.arraycopy(longToBytes(leastSignificantBits), 0, id, 8, 8);
    FileOutputStream fos = null;
    try
    {
      fos = new FileOutputStream(newFile);
      fos.write(node.getNodeType().equals(TreeNodeType.LeafNode) ? 1 : 0);
      fos.write(new byte[56]);
    } catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException(e);
    } finally
    {
      if (fos != null)
      {
        try
        {
          fos.close();
        } catch (IOException e)
        {
        }
      }
    }
    return new Bytes(id);
  }

  @Override
  public BTreeNode<BytesWrapper> getRoot()
  {
    File metaFile = new File(baseFolderPath, ".metadata");
    if (!metaFile.exists())
    {
      FileOutputStream fos = null;
      try
      {
        metaFile.createNewFile();
        fos = new FileOutputStream(metaFile);
        BTreeNode<BytesWrapper> root = new BTreeLeafNode<BytesWrapper, BytesWrapper>(this);
        fos.write(((Bytes) root.getId()).getBytes());
      } catch (IOException e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
        throw new RuntimeException(e);
      } finally
      {
        if (fos != null)
        {
          try
          {
            fos.close();
          } catch (IOException e)
          {
          }
        }
      }
    }
    byte[] id = readFile(metaFile);
    return getNode(new Bytes(id));
  }

  @Override
  public void setRoot(BTreeNode<BytesWrapper> node)
  {
    File metaFile = new File(baseFolderPath, ".metadata");
    OutputStream os = null;
    try
    {
      os = new FileOutputStream(metaFile);
      // TODO Write completely
      os.write(((Bytes) node.getId()).getBytes());
    } catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException(e);
    } finally
    {
      if (os != null)
      {
        try
        {
          os.close();
        } catch (IOException e)
        {
        }
      }
    }
  }

  @Override
  public BTreeNode<BytesWrapper> getNode(Object nodeId)
  {
    BTreeNode<BytesWrapper> cached = cache.get(nodeId);
    if (cached != null)
    {
//      System.out.println("hit !");
      return cached;
    }
    return readNode(((Bytes) nodeId).getBytes());
  }

  @Override
  public BTreeNode<BytesWrapper> getChild(BTreeInnerNode<BytesWrapper> node, int index)
  {
    return (BTreeNode<BytesWrapper>) this.getNode(node.children[index]);
  }

  @Override
  public void setParent(BTreeNode<BytesWrapper> node, Object parentId)
  {
    if (tx != null)
    {
      tx.addNode(node);
      node.parentNodeId = parentId;
      return;
    }
    setRef(node, 1, parentId);
    node.parentNodeId = parentId;
  }

  @Override
  public void setChild(BTreeInnerNode<BytesWrapper> node, int index,
      BTreeNode<BytesWrapper> child)
  {
    if (tx != null)
    {
      tx.addNode(node);
      tx.addNode(child);
      if (child == null)
      {
        node.children[index] = null;
      } else
      {
        node.children[index] = child.getId();
      }
      return;
    }
    if (child == null)
    {
      node.children[index] = null;
    } else
    {
      node.children[index] = child.getId();
      child.setParent(this, node);
    }
    saveParts(node);
  }

  @Override
  public void setKey(BTreeNode<BytesWrapper> node, int index, BytesWrapper key)
  {
    if (tx != null)
    {
      tx.addNode(node);
      node.keys[index] = key;
      return;
    }
    node.keys[index] = key;
    saveParts(node);
  }

  @Override
  public void setKeyCount(BTreeNode<BytesWrapper> node, int newKeyCount)
  {
    if (tx != null)
    {
      tx.addNode(node);
      node.keyCount = newKeyCount;
      return;
    }
    node.keyCount = newKeyCount;
    File nodeFile = new File(baseFolderPath + "/data"
        + partsToFile(idToParts(((Bytes) node.getId()).getBytes())));
    try
    {
      // TODO Diff rw and rwd
      RandomAccessFile raf = new RandomAccessFile(nodeFile, "rwd");
      raf.seek(49);// /System.out.println(node + " " +
                   // java.util.Arrays.toString(((Bytes)
                   // node.getId()).getBytes()) + " " + node.keyCount);
      raf.write(intToBytes(node.keyCount));
      raf.close();
    } catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setValue(BTreeLeafNode<BytesWrapper, BytesWrapper> node, int index, BytesWrapper value)
  {
    if (tx != null)
    {
      tx.addNode(node);
      node.values[index] = value;
      return;
    }
    node.values[index] = value;
    saveParts(node);
  }

  @Override
  public void setLeftSibling(BTreeNode<BytesWrapper> node, Object siblingId)
  {
    if (tx != null)
    {
      tx.addNode(node);
      node.leftSiblingId = siblingId;
      return;
    }
    setRef(node, 17, siblingId);
    node.leftSiblingId = siblingId;
  }

  @Override
  public void setRightSibling(BTreeNode<BytesWrapper> node, Object siblingId)
  {
    if (tx != null)
    {
      tx.addNode(node);
      node.rightSiblingId = siblingId;
      return;
    }
    setRef(node, 33, siblingId);
    node.rightSiblingId = siblingId;
  }

  @Override
  public Tx<BytesWrapper> getTx()
  {
    tx = new DefaultTx();
    return tx;
  }

  @SuppressWarnings("unchecked")
  private BTreeNode<BytesWrapper> readNode(byte[] id)
  {
    File nodeFile = new File(baseFolderPath + "/data"
        + partsToFile(idToParts(id)));
    byte[] content = readFile(nodeFile);
    int offset = 0;
    boolean isLeaf = content[offset] != 0;
    offset += 1;
    final byte[] parent = new byte[16];
    System.arraycopy(content, offset, parent, 0, 16);
    offset += 16;
    final byte[] left = new byte[16];
    System.arraycopy(content, offset, left, 0, 16);
    offset += 16;
    final byte[] right = new byte[16];
    System.arraycopy(content, offset, right, 0, 16);
    offset += 16;
    int keyCount = bytesToInt(content, offset);// /System.out.println(java.util.Arrays.toString(id)
                                               // + "!" + keyCount);
    offset += 4;
    final Object[] keys = new Object[BTreeNode.ORDER + 1];
    for (int i = 0; i < keyCount; i++)
    {
      int index = bytesToInt(content, offset);
      int keyLength = bytesToInt(content, offset + 4);
      byte[] keyBytes = new byte[keyLength];
      System.arraycopy(content, offset + 8, keyBytes, 0, keyLength);
      // FIXME Deserializing K
      keys[index] = new BytesWrapper(keyBytes);
      // System.out.println("K: " + java.util.Arrays.toString(keyBytes));
      offset += 4 + 4 + keyLength;
    }
    final Object[] children;
    final Object[] values;
    if (isLeaf)
    {
      children = null;
      int valueCount = bytesToInt(content, offset);
      offset += 4;
      values = new BytesWrapper[keys.length];
      for (int i = 0; i < valueCount; i++)
      {
        int index = bytesToInt(content, offset);
        int valueLength = bytesToInt(content, offset + 4);
        byte[] valueBytes = new byte[valueLength];
        System.arraycopy(content, offset + 8, valueBytes, 0, valueLength);
        // FIXME Deserializing V
        values[index] = new BytesWrapper(valueBytes);
        offset += 8 + valueLength;
      }
    } else
    {
      values = null;
      int childCount = bytesToInt(content, offset);
      offset += 4;
      children = new byte[keys.length + 1][];
      for (int i = 0; i < childCount; i++)
      {
        int index = bytesToInt(content, offset);
        byte[] childBytes = new byte[16];
        System.arraycopy(content, offset + 4, childBytes, 0, 16);
        children[index] = childBytes;
        offset += 20;
      }
    }
    final BTreeNode<BytesWrapper> node = isLeaf ? new BTreeLeafNode<BytesWrapper, BytesWrapper>(this,
        new Bytes(id)) : new BTreeInnerNode<BytesWrapper>(this, new Bytes(id));
    node.parentNodeId = zeroToNull(parent);
    node.leftSiblingId = zeroToNull(left);
    node.rightSiblingId = zeroToNull(right);
    node.keys = keys;
    node.keyCount = keyCount;
    if (isLeaf)
    {
      ((BTreeLeafNode<BytesWrapper, BytesWrapper>) node).values = values;
    } else
    {
      ((BTreeInnerNode<BytesWrapper>) node).children = children;
    }
    cache.put(new Bytes(id), node);
    return node;
  }

  private static byte[] readFile(File file)
  {
    final byte[] bytes = new byte[(int) file.length()];
    try
    {
      new FileInputStream(file).getChannel().transferTo(0, bytes.length,
          new WritableByteChannel()
          {
            int offset = 0;

            @Override
            public boolean isOpen()
            {
              return true;
            }

            @Override
            public void close() throws IOException
            {
            }

            @Override
            public int write(ByteBuffer src) throws IOException
            {
              int wrotten = src.remaining();
              src.get(bytes, offset, wrotten);
              offset += wrotten;
              return wrotten;
            }
          });
    } catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return bytes;
  }

  private void setRef(BTreeNode<BytesWrapper> node, int offset, Object ref)
  {
    byte[] refBytes = nullToZero(((Bytes) ref).getBytes());
    File nodeFile = new File(baseFolderPath + "/data"
        + partsToFile(idToParts(((Bytes) node.getId()).getBytes())));
    try
    {
      // TODO Diff rw and rwd
      RandomAccessFile raf = new RandomAccessFile(nodeFile, "rwd");
      raf.seek(offset);
      raf.write(refBytes);
      raf.close();
    } catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private void saveParts(BTreeNode<BytesWrapper> node)
  {
    File nodeFile = new File(baseFolderPath + "/data"
        + partsToFile(idToParts(((Bytes) node.getId()).getBytes())));
    try
    {
      // TODO Diff rw and rwd
      RandomAccessFile raf = new RandomAccessFile(nodeFile, "rwd");
      raf.seek(49);
      raf.write(intToBytes(node.keyCount));// /System.out.println(java.util.Arrays.toString(((Bytes)
                                           // node.getId()).getBytes()) + "?" +
                                           // node.keyCount);
      for (int i = 0; i < node.keys.length; i++)
      {
        BytesWrapper keyBytes_ = (BytesWrapper) node.keys[i];
        if (keyBytes_ != null)
        {
          raf.write(intToBytes(i));
          raf.write(intToBytes(keyBytes_.getLength()));
          // System.out.println(">: " + java.util.Arrays.toString(key_));
          for (byte[] bs : keyBytes_.getArrays())
          {
            raf.write(bs);
          }
        }
      }
      if (node.getNodeType().equals(TreeNodeType.InnerNode))
      {
        Object[] children = ((BTreeInnerNode<BytesWrapper>) node).children;
        int childCount = 0;
        for (Object childId_ : children)
        {
          if (childId_ != null)
          {
            childCount++;
          }
        }
        raf.write(intToBytes(childCount));
        for (int index_ = 0; index_ < children.length; index_++)
        {
          Bytes childIdBytes_ = (Bytes) children[index_];
          if (childIdBytes_ != null)
          {
            byte[] childId_ = childIdBytes_.getBytes();
            raf.write(intToBytes(index_));
            raf.write(childId_);
          }
        }
      } else
      {
        Object[] values = ((BTreeLeafNode<BytesWrapper, BytesWrapper>) node).values;
        int valueCount = 0;
        for (Object value : values)
        {
          if (value != null)
          {
            valueCount++;
          }
        }
        raf.write(intToBytes(valueCount));
        for (int index_ = 0; index_ < values.length; index_++)
        {
          // FIXME Deserializing
          BytesWrapper value = (BytesWrapper) values[index_];
          if (value != null)
          {
            raf.write(intToBytes(index_));
            raf.write(intToBytes(value.getLength()));
            for (byte[] bs : value.getArrays())
            {
              raf.write(bs);
            }
          }
        }
      }
      raf.setLength(raf.getFilePointer());

      raf.close();
    } catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private void saveAll(BTreeNode<BytesWrapper> node)
  {
    File nodeFile = new File(baseFolderPath + "/data"
        + partsToFile(idToParts(((Bytes) node.getId()).getBytes())));
    OutputStream os = null;
    try
    {
      os = new FileOutputStream(nodeFile);
      os.write(node.getNodeType().equals(TreeNodeType.InnerNode) ? 0 : 1);
      os.write(nullToZero(node.parentNodeId == null ? null
          : ((Bytes) node.parentNodeId).getBytes()));
      os.write(nullToZero(node.leftSiblingId == null ? null
          : ((Bytes) node.leftSiblingId).getBytes()));
      os.write(nullToZero(node.rightSiblingId == null ? null
          : ((Bytes) node.rightSiblingId).getBytes()));
      os.write(intToBytes(node.keyCount));// /System.out.println(java.util.Arrays.toString(((Bytes)
                                          // node.getId()).getBytes()) + "?" +
                                          // node.keyCount);
      for (int i = 0; i < node.keys.length; i++)
      {
        BytesWrapper keyBytes_ = (BytesWrapper) node.keys[i];
        if (keyBytes_ != null)
        {
          os.write(intToBytes(i));
          os.write(intToBytes(keyBytes_.getLength()));
          // System.out.println(">: " + java.util.Arrays.toString(key_));
          for (byte[] bs : keyBytes_.getArrays())
          {
            os.write(bs);
          }
        }
      }
      if (node.getNodeType().equals(TreeNodeType.InnerNode))
      {
        Object[] children = ((BTreeInnerNode<BytesWrapper>) node).children;
        int childCount = 0;
        for (Object childId_ : children)
        {
          if (childId_ != null)
          {
            childCount++;
          }
        }
        os.write(intToBytes(childCount));
        for (int index_ = 0; index_ < children.length; index_++)
        {
          Bytes childIdBytes_ = (Bytes) children[index_];
          if (childIdBytes_ != null)
          {
            byte[] childId_ = childIdBytes_.getBytes();
            os.write(intToBytes(index_));
            os.write(childId_);
          }
        }
      } else
      {
        Object[] values = ((BTreeLeafNode<BytesWrapper, BytesWrapper>) node).values;
        int valueCount = 0;
        for (Object value : values)
        {
          if (value != null)
          {
            valueCount++;
          }
        }
        os.write(intToBytes(valueCount));
        for (int index_ = 0; index_ < values.length; index_++)
        {
          // FIXME Deserializing
          BytesWrapper value = (BytesWrapper) values[index_];
          if (value != null)
          {
            os.write(intToBytes(index_));
            os.write(intToBytes(value.getLength()));
            for (byte[] bs : value.getArrays())
            {
              os.write(bs);
            }
          }
        }
      }
    } catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException(e);
    } finally
    {
      if (os != null)
      {
        try
        {
          os.close();
        } catch (IOException e)
        {
        }
      }
    }
  }

  private int[] idToParts(byte[] id)
  {
    return new int[]
    { (int) ((bytesToInt(id, 0) & 0xffffffffL) % MAX_CHILDREN),
        (int) ((bytesToInt(id, 4) & 0xffffffffL) % MAX_CHILDREN),
        (int) ((bytesToInt(id, 8) & 0xffffffffL) % MAX_CHILDREN),
        (int) ((bytesToInt(id, 12) & 0xffffffffL) % MAX_CHILDREN) };
  }

  private String partsToFolder(int[] parts)
  {
    return "/" + parts[0] + "/" + parts[1] + "/" + parts[2];
  }

  private String partsToFile(int[] parts)
  {
    return partsToFolder(parts) + "/" + parts[3];
  }

  private Object zeroToNull(byte[] bytes)
  {
    for (byte b : bytes)
    {
      if (b != 0)
      {
        return bytes;
      }
    }
    return null;
  }

  private byte[] nullToZero(byte[] bytes)
  {
    return bytes == null ? new byte[16] : bytes;
  }

  public static void main(String[] args)
  {

    UUID uuid = UUID.randomUUID();
    System.out.println(uuid.toString());
    long mostSignificantBits = uuid.getMostSignificantBits();
    System.out.println(Long.toHexString(mostSignificantBits));
    long leastSignificantBits = uuid.getLeastSignificantBits();
    System.out.println(Long.toHexString(leastSignificantBits));
    System.out.println(mostSignificantBits);
    System.out.println(leastSignificantBits);
    int[] parts = new int[]
    { (int) ((mostSignificantBits & 0xffffffffL) % MAX_CHILDREN),
        (int) ((mostSignificantBits >>> 32) % MAX_CHILDREN),
        (int) ((leastSignificantBits & 0xffffffffL) % MAX_CHILDREN),
        (int) ((leastSignificantBits >>> 32) % MAX_CHILDREN) };
    System.out.println(java.util.Arrays.toString(parts));
    byte[] id = new byte[16];
    System.arraycopy(longToBytes(mostSignificantBits), 0, id, 0, 8);
    System.arraycopy(longToBytes(leastSignificantBits), 0, id, 8, 8);
    parts = new int[]
    { (int) ((bytesToInt(id, 0) & 0xffffffffL) % MAX_CHILDREN),
        (int) ((bytesToInt(id, 4) & 0xffffffffL) % MAX_CHILDREN),
        (int) ((bytesToInt(id, 8) & 0xffffffffL) % MAX_CHILDREN),
        (int) ((bytesToInt(id, 12) & 0xffffffffL) % MAX_CHILDREN) };
    System.out.println(java.util.Arrays.toString(parts));
  }

  private final class DefaultTx implements Tx<BytesWrapper>
  {

    Map<Bytes, BTreeNode<BytesWrapper>> map = new HashMap<Bytes, BTreeNode<BytesWrapper>>();

    @Override
    public void addNode(BTreeNode<BytesWrapper> node)
    {
      if (node == null)
      {
        return;
      }
      map.put((Bytes) node.getId(), node);
    }

    @Override
    public void commit()
    {
      DefaultDisk.this.tx = null;
      for (BTreeNode<BytesWrapper> node : map.values())
      {
        DefaultDisk.this.saveAll(node);
      }
    }

  }

}
