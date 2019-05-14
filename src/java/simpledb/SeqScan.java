package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

	private static final long serialVersionUID = 1L;

	private String alias;
	private TupleDesc description;
	private TransactionId tid;
	private int tableId;
	private DbFileIterator dbIter;

	/**
	 * Creates a sequential scan over the specified table as a part of the specified
	 * transaction.
	 *
	 * @param tid        The transaction this scan is running as a part of.
	 * @param tableid    the table to scan.
	 * @param tableAlias the alias of this table (needed by the parser); the
	 *                   returned tupleDesc should have fields with name
	 *                   tableAlias.fieldName (note: this class is not responsible
	 *                   for handling a case where tableAlias or fieldName are null.
	 *                   It shouldn't crash if they are, but the resulting name can
	 *                   be null.fieldName, tableAlias.null, or null.null).
	 */
	public SeqScan(TransactionId tid, int tableid, String tableAlias) {
		// some code goes here
		init(tid, tableid, tableAlias);
		HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
		this.dbIter = hf.iterator(tid);
	}

	private void init(TransactionId tid, int tableId, String tableAlias) {
		this.tid = tid;
		this.tableId = tableId;
		alias = tableAlias;
		TupleDesc desc;
		try {
			desc = Database.getCatalog().getTupleDesc(tableId);

		} catch (NoSuchElementException e) {
			e.printStackTrace();
			System.out.println("No such a tuple description for table with ID: " + tableId);
			return;
		}
		description = desc.makeCopyWithAlias(alias);
	}

	/**
	 * @return return the table name of the table the operator scans. This should be
	 *         the actual name of the table in the catalog of the database
	 */
	public String getTableName() {
		return Database.getCatalog().getTableName(tableId);
	}

	/**
	 * @return Return the alias of the table this operator scans.
	 */
	public String getAlias() {
		// some code goes here
		return alias;
	}

	/**
	 * Reset the tableid, and tableAlias of this operator.
	 * 
	 * @param tableid    the table to scan.
	 * @param tableAlias the alias of this table (needed by the parser); the
	 *                   returned tupleDesc should have fields with name
	 *                   tableAlias.fieldName (note: this class is not responsible
	 *                   for handling a case where tableAlias or fieldName are null.
	 *                   It shouldn't crash if they are, but the resulting name can
	 *                   be null.fieldName, tableAlias.null, or null.null).
	 */
	public void reset(int tableid, String tableAlias) {
		// some code goes here
		init(tid, tableid, tableAlias);
	}

	public SeqScan(TransactionId tid, int tableId) {
		this(tid, tableId, Database.getCatalog().getTableName(tableId));
	}

	public void open() throws DbException, TransactionAbortedException {
		// some code goes here
		dbIter.open();
	}

	/**
	 * Returns the TupleDesc with field names from the underlying HeapFile, prefixed
	 * with the tableAlias string from the constructor. This prefix becomes useful
	 * when joining tables containing a field(s) with the same name. The alias and
	 * name should be separated with a "." character (e.g., "alias.fieldName").
	 *
	 * @return the TupleDesc with field names from the underlying HeapFile, prefixed
	 *         with the tableAlias string from the constructor.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		return description;
	}

	public boolean hasNext() throws TransactionAbortedException, DbException {
		// some code goes here
		return dbIter.hasNext();
	}

	public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
		// some code goes here
		return dbIter.next();
	}

	public void close() {
		// some code goes here
		dbIter.close();
	}

	public void rewind() throws DbException, NoSuchElementException, TransactionAbortedException {
		// some code goes here
		dbIter.rewind();
	}
}