package simpledb;

import java.util.*;
import java.io.*;
import java.lang.Math;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

	final HeapPageId pid;
	final TupleDesc td;
	final byte hdrs[];
	final Tuple tuples[];
	final int numSlots;

	byte[] oldData;
	private final Byte oldDataLock = new Byte((byte) 0);
	private int emptySlotIdx;
	private boolean isPageDirty;
	private TransactionId lastDirtyTxn;

	/**
	 * Create a HeapPage from a set of bytes of data read from disk. The format of a
	 * HeapPage is a set of hdrs bytes indicating the slots of the page that are in
	 * use, some number of tuple slots. Specifically, the number of tuples is equal
	 * to:
	 * <p>
	 * floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
	 * <p>
	 * where tuple size is the size of tuples in this database table, which can be
	 * determined via {@link Catalog#getTupleDesc}. The number of 8-bit hdrs words
	 * is equal to:
	 * <p>
	 * ceiling(no. tuple slots / 8)
	 * <p>
	 * 
	 * @see Database#getCatalog
	 * @see Catalog#getTupleDesc
	 * @see BufferPool#getPageSize()
	 */
	// when a HeapPage is created, its data are given in an array
	public HeapPage(HeapPageId id, byte[] data) throws IOException {
		this.pid = id;
		this.td = Database.getCatalog().getTupleDesc(id.getTableId());
		this.numSlots = getNumTuples();
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
		// allocate and read the hdrs slots of this page
		hdrs = new byte[getHeaderSize()];
		for (int i = 0; i < hdrs.length; i++)
			hdrs[i] = dis.readByte();

		tuples = new Tuple[numSlots];
		this.emptySlotIdx = 0;
		try {
			// allocate and read the actual records of this page
			for (int i = 0; i < tuples.length; i++) {
				tuples[i] = readNextTuple(dis, i);
				if (!isSlotUsed(i))
					this.emptySlotIdx++;
			}
		} catch (NoSuchElementException e) {
			e.printStackTrace();
		}
		dis.close();
		isPageDirty = false;
		lastDirtyTxn = null;
		setBeforeImage();
	}

	/**
	 * Retrieve the number of tuples on this page.
	 * 
	 * @return the number of tuples on this page
	 */
	private int getNumTuples() {
		// some code goes here
		// floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
		int tupleSize = td.getSize();
		return (int) Math.floor((BufferPool.getPageSize() * 8) / (double) (tupleSize * 8 + 1));
	}

	/**
	 * Computes the number of bytes in the hdrs of a page in a HeapFile with each
	 * tuple occupying tupleSize bytes
	 * 
	 * @return the number of bytes in the hdrs of a page in a HeapFile with each
	 *         tuple occupying tupleSize bytes
	 */
	private int getHeaderSize() {
		// some code goes here
		return (int) Math.ceil((double) numSlots / 8.0);
	}

	/**
	 * Return a view of this page before it was modified -- used by recovery
	 */
	public HeapPage getBeforeImage() {
		try {
			byte[] oldDataRef = null;
			synchronized (oldDataLock) {
				oldDataRef = oldData;
			}
			return new HeapPage(pid, oldDataRef);
		} catch (IOException e) {
			e.printStackTrace();
			// should never happen -- we parsed it OK before!
			System.exit(1);
		}
		return null;
	}

	public void setBeforeImage() {
		synchronized (oldDataLock) {
			oldData = getPageData().clone();
		}
	}

	/**
	 * @return the PageId associated with this page.
	 */
	public HeapPageId getId() {
		// some code goes here
		return pid;
	}

	/**
	 * Suck up tuples from the source file.
	 */
	private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
		// if associated bit is not set, read forward to the next tuple, and
		// return null.
		if (!isSlotUsed(slotId)) {
			for (int i = 0; i < td.getSize(); i++) {
				try {
					dis.readByte();
				} catch (IOException e) {
					throw new NoSuchElementException("error reading empty tuple");
				}
			}
			return null;
		}

		// read fields in the tuple
		Tuple t = new Tuple(td);
		RecordId rid = new RecordId(pid, slotId);
		t.setRecordId(rid);
		try {
			for (int j = 0; j < td.numFields(); j++) {
				Field f = td.getFieldType(j).parse(dis); // data is in the Field
				t.setField(j, f);
			}
		} catch (java.text.ParseException e) {
			e.printStackTrace();
			throw new NoSuchElementException("parsing error!");
		}

		return t;
	}

	/**
	 * Generates a byte array representing the contents of this page. Used to
	 * serialize this page to disk.
	 * <p>
	 * The invariant here is that it should be possible to pass the byte array
	 * generated by getPageData to the HeapPage constructor and have it produce an
	 * identical HeapPage object.
	 *
	 * @see #HeapPage
	 * @return A byte array correspond to the bytes of this page.
	 */
	public byte[] getPageData() {
		int len = BufferPool.getPageSize();
		ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
		DataOutputStream dos = new DataOutputStream(baos);

		// create the hdrs of the page
		for (int i = 0; i < hdrs.length; i++) {
			try {
				dos.writeByte(hdrs[i]);
			} catch (IOException e) {
				// this really shouldn't happen
				e.printStackTrace();
			}
		}

		// create the tuples
		for (int i = 0; i < tuples.length; i++) {

			// empty slot
			if (!isSlotUsed(i)) {
				for (int j = 0; j < td.getSize(); j++) {
					try {
						dos.writeByte(0);
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
				continue;
			}

			// non-empty slot
			for (int j = 0; j < td.numFields(); j++) {
				Field f = tuples[i].getField(j);
				try {
					f.serialize(dos);

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// padding
		int zerolen = BufferPool.getPageSize() - (hdrs.length + td.getSize() * tuples.length); // - numSlots *
																								// td.getSize();
		byte[] zeroes = new byte[zerolen];
		try {
			dos.write(zeroes, 0, zerolen);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			dos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return baos.toByteArray();
	}

	/**
	 * Static method to generate a byte array corresponding to an empty HeapPage.
	 * Used to add new, empty pages to the file. Passing the results of this method
	 * to the HeapPage constructor will create a HeapPage with no valid tuples in
	 * it.
	 *
	 * @return The returned ByteArray.
	 */
	public static byte[] createEmptyPageData() {
		int len = BufferPool.getPageSize();
		return new byte[len]; // all 0
	}

	/**
	 * Delete the specified tuple from the page; the corresponding hdrs bit should
	 * be updated to reflect that it is no longer stored on any page.
	 * 
	 * @throws DbException if this tuple is not on this page, or tuple slot is
	 *                     already empty.
	 * @param t The tuple to delete
	 */
	public void deleteTuple(Tuple t) throws DbException {
		// some code goes here
		// check if page ID matches
		RecordId tupRecord = t.getRecordId();
		if (!tupRecord.getPageId().equals(pid))
			throw new DbException("Tuple to delete doesn't seem to be on this page");
		int tupNum = tupRecord.getTupleNumber();
		if (!isSlotUsed(tupNum))
			throw new DbException("Slot is already empty");
		markSlotUsed(tupNum, false);
		tuples[tupNum] = null;
	}

	/**
	 * Adds the specified tuple to the page; the tuple should be updated to reflect
	 * that it is now stored on this page.
	 * 
	 * @throws DbException if the page is full (no empty slots) or tupledesc is
	 *                     mismatch.
	 * @param t The tuple to add.
	 */
	public void insertTuple(Tuple t) throws DbException {
		// some code goes here
		if (!t.getTupleDesc().equals(td))
			throw new DbException("Tuple description is mismatch.");

		if (getNumEmptySlots() == 0)
			throw new DbException("Page is full");

		int slot = getNextEmptySlot();
		assert slot != -1;
		// set the record ID of the current tuple using the current page information
		if (t.getRecordId() == null) {
			RecordId rid = new RecordId(getId(), slot);
			t.setRecordId(rid);
		} else {
			t.getRecordId().setPageId(getId());
			t.getRecordId().setTupleNumber(slot);
		}
		markSlotUsed(slot, true);
		tuples[slot] = t;
	}

	/**
	 * Marks this page as dirty/not dirty and record that transaction that did the
	 * dirtying
	 */
	public void markDirty(boolean dirty, TransactionId tid) {
		// some code goes here
		isPageDirty = dirty;
		if (isPageDirty)
			lastDirtyTxn = tid;
	}

	/**
	 * Returns the tid of the transaction that last dirtied this page, or null if
	 * the page is not dirty
	 */
	public TransactionId isDirty() {
		// some code goes here
		// Not necessary for lab1
		return (isPageDirty ? lastDirtyTxn : null);
	}

	/**
	 * Returns the number of empty slots on this page.
	 */
	public int getNumEmptySlots() {
		// some code goes here
		return emptySlotIdx;
	}

	/**
	 * Returns true if associated slot on this page is filled.
	 */
	public boolean isSlotUsed(int i) {
		// some code goes here
		byte byteVal = hdrs[i / 8];
		return isNthBitSet(byteVal, i % 8);
	}

	// return -1 if the page is full
	public int getNextEmptySlot() {
		for (int i = 0; i < numSlots; i++) {
			if (!isSlotUsed(i))
				return i;
		}
		return -1;
	}

	/**
	 * Abstraction to fill or clear a slot on this page.
	 */
	private void markSlotUsed(int i, boolean value) {
		// some code goes here
		// not necessary for lab1
		if (isSlotUsed(i) ^ value)
			hdrs[i / 8] ^= (1 << (i % 8));
		emptySlotIdx += (value ? -1 : 1);
	}

	/**
	 * @return an iterator over all tuples on this page (calling remove on this
	 *         iterator throws an UnsupportedOperationException) (note that this
	 *         iterator shouldn't return tuples in empty slots!)
	 */
	public Iterator<Tuple> iterator() {
		// some code goes here
		List<Tuple> tuplesOnPage = new LinkedList<>();
		for (int i = 0; i < tuples.length; i++) {
			if (isSlotUsed(i))
				tuplesOnPage.add(tuples[i]);
		}
		return tuplesOnPage.iterator();
	}

	private boolean isNthBitSet(byte byteVal, int n) {
		return ((byteVal >> n) & 1) == 1;
	}

	public void printHeader() {
		System.out.println("hdrs: ");
		for (int i = 0; i < hdrs.length; i++) {
			byte b = hdrs[i];
			StringBuilder binaryStringBuilder = new StringBuilder();
			for (int j = 7; j >= 0; j--)
				binaryStringBuilder.append(((0x80 >>> j) & b) == 0 ? '0' : '1');
			System.out.println(binaryStringBuilder.toString());
		}
		System.out.println();
	}
}
