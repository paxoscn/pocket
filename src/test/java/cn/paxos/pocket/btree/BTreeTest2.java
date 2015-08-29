package cn.paxos.pocket.btree;

public class BTreeTest2
{
  public static void main(String args[])
  {
    BytesBTree tree = new BytesBTree();

    // test for the btree on assignment4
    // tree.insert(1);
    // tree.insert(78);
    // tree.insert(37);
    // tree.insert(150);
    // tree.insert(35);
    // tree.insert(145);
    // tree.insert(19);
    // tree.insert(24);
    //
    // tree.insert(10);
    // tree.insert(210);
    // tree.insert(17);
    // tree.insert(20);
    // tree.insert(30);
    // tree.insert(201);
    //
    // tree.insert(140);
    // tree.insert(207);
    // tree.insert(120);
    // tree.insert(5);
    //
    // tree.insert(115);
    // tree.insert(51);
    // tree.insert(40);
    // tree.insert(7);

    // test for the btree on notes
    // tree = new BytesBTree();
    tree.insert(10);
    tree.insert(48);
    tree.insert(23);
    tree.insert(33);
    tree.insert(12);

    tree.insert(50);

    tree.insert(15);
    tree.insert(18);
    tree.insert(20);
    tree.insert(21);
    tree.insert(31);
    tree.insert(45);
    tree.insert(47);
    tree.insert(52);

    tree.insert(30);

    tree.insert(19);
    tree.insert(22);

    tree.insert(11);
    tree.insert(13);
    tree.insert(16);
    tree.insert(17);

    tree.insert(1);
    tree.insert(2);
    tree.insert(3);
    tree.insert(4);
    tree.insert(5);
    tree.insert(6);
    tree.insert(7);
    tree.insert(8);
    tree.insert(9);

    long ss = System.currentTimeMillis();
    Iterable<BytesWrapper> s = tree.search(29, 53);
    System.out.println(">>" + (System.currentTimeMillis() - ss));
    for (BytesWrapper bs : s)
    {
      System.out.println(bytesToInt(bs.getArrays().get(0), 0));
    }

    // tree.remove(18);
    // tree.remove(12);
    // tree.remove(33);
    //
    // tree.remove(10);
    // tree.remove(22);
    // tree.remove(21);
    //
    // tree.remove(12);
    // tree.remove(15);
    // tree.remove(18);
    // tree.remove(19);
    // tree.remove(20);
    //
    // tree.remove(23);
    // tree.remove(30);
    // tree.remove(31);
    //
    // tree.remove(45);
    // tree.remove(47);
    // tree.remove(48);
    // tree.remove(50);
    // tree.remove(52);

    return;
  }

  private static int bytesToInt(byte[] bytes, int bytesOffset)
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

class BytesBTree extends BTree<BytesWrapper, BytesWrapper>
{
  public BytesBTree()
  {
    super(new DefaultDisk("/tmp/bptree"));
  }

  public void insert(int i)
  {
    byte[] b = intToBytes(i);
    long ss = System.currentTimeMillis();
    this.insert(new BytesWrapper(b), new BytesWrapper(intToBytes(i * 10)));
    System.out.println(">" + (System.currentTimeMillis() - ss));
  }

  public void remove(int i)
  {
    byte[] b = intToBytes(i);
    this.delete(new BytesWrapper(b));
  }

  public Iterable<BytesWrapper> search(int start, int stop)
  {
    byte[] b1 = intToBytes(start);
    byte[] b2 = intToBytes(stop);
    return super.search(new BytesWrapper(b1), new BytesWrapper(b2));
  }

  private static byte[] intToBytes(int value)
  {
    byte[] bytes = new byte[4];
    for (int i = bytes.length - 1; i >= 0; i--)
    {
      int offset = i * 8;
      bytes[i] = (byte) (value >> offset);
    }
    return bytes;
  }

}