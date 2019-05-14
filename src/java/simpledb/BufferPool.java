package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool checks that the transaction has the appropriate locks to
 * read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
	/** Bytes per page, including header. */
	private static final int DEFAULT_PAGE_SIZE = 4096;

	private static int pageSize = DEFAULT_PAGE_SIZE;

	/**
	 * Default number of pages passed to the constructor. This is used by other
	 * classes. BufferPool should use the numPages argument to the constructor
	 * instead.
	 */
	public static final int DEFAULT_PAGES = 50;
	private AtomicInteger numPages = new AtomicInteger(DEFAULT_PAGES);
	private final Map<PageId, Page> pageMap = new HashMap<>();
	private final Map<TransactionId, Set<PageId>> transactionsToDirtyFlushedPages = new HashMap<>();
	private final LockManager lockManager = LockManager.create();

	/**
	 * Creates a BufferPool that caches up to numPages pages.
	 *
	 * @param numPages maximum number of pages in this buffer pool.
	 */
	public BufferPool(int numPages) {
		this.numPages = new AtomicInteger(numPages);
	}

	public static int getPageSize() {
		return pageSize;
	}

	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void setPageSize(int pageSize) {
		BufferPool.pageSize = pageSize;
	}

	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void resetPageSize() {
		BufferPool.pageSize = DEFAULT_PAGE_SIZE;
	}

	/**
	 * Retrieve the specified page with the associated permissions. Will acquire a
	 * lock and may block if that lock is held by another transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool. If it is present,
	 * it should be returned. If it is not present, it should be added to the buffer
	 * pool and returned. If there is insufficient space in the buffer pool, a page
	 * should be evicted and the new page should be added in its place.
	 *
	 * @param tid  the ID of the transaction requesting the page
	 * @param pid  the ID of the requested page
	 * @param perm the requested permissions on the page
	 */
	public Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException {
		lockManager.acquireLock(tid, pid, perm);
		if (!this.pageMap.containsKey(pid) && this.pageMap.size() == this.numPages.get()) {
			evictPage();
		}
		if (!this.pageMap.containsKey(pid)) {
			Catalog dbCatalog = Database.getCatalog();
			int tableId = pid.getTableId();
			this.pageMap.put(pid, dbCatalog.getDatabaseFile(tableId).readPage(pid));
		}

		return this.pageMap.get(pid);
	}

	/**
	 * Releases the lock on a page. Calling this is very risky, and may result in
	 * wrong behavior. Think hard about who needs to call this and why, and why they
	 * can run the risk of calling it.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 * @param pid the ID of the page to unlock
	 */
	public void releasePage(TransactionId tid, PageId pid) {
		// some code goes here
		// not necessary for lab1|lab2
		lockManager.releasePage(tid, pid);
	}

	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 */
	public void transactionComplete(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
		transactionComplete(tid, true);
	}

	/** Return true if the specified transaction has a lock on the specified page */
	public boolean holdsLock(TransactionId tid, PageId pid) {
		// some code goes here
		// not necessary for lab1|lab2
		return lockManager.holdsLock(tid, pid);
	}

	/**
	 * Commit or abort a given transaction; release all locks associated to the
	 * transaction.
	 *
	 * @param tid    the ID of the transaction requesting the unlock
	 * @param commit a flag indicating whether we should commit or abort
	 */
	public void transactionComplete(TransactionId tid, boolean commit) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
		Set<PageId> dirtyFlushedPages = transactionsToDirtyFlushedPages.get(tid);
		for (PageId pid : pageMap.keySet()) {
			Page p = pageMap.get(pid);
			if (tid.equals(p.isDirty())) {
				if (commit) {
					flushPage(pid);
					p.setBeforeImage();
				} else {
					pageMap.put(pid, p.getBeforeImage());
					p.markDirty(false, null);
				}
			} else if (dirtyFlushedPages != null && dirtyFlushedPages.contains(pid)) {
				p.setBeforeImage();
			}
		}
		transactionsToDirtyFlushedPages.remove(tid);
		lockManager.releasePages(tid);
	}

	/**
	 * Add a tuple to the specified table on behalf of transaction tid. Will acquire
	 * a write lock on the page the tuple is added to and any other pages that are
	 * updated (Lock acquisition is not needed for lab2). May block if the lock(s)
	 * cannot be acquired.
	 * 
	 * Marks any pages that were dirtied by the operation as dirty by calling their
	 * markDirty bit, and adds versions of any pages that have been dirtied to the
	 * cache (replacing any existing versions of those pages) so that future
	 * requests see up-to-date pages.
	 *
	 * @param tid     the transaction adding the tuple
	 * @param tableId the table to add the tuple to
	 * @param t       the tuple to add
	 */
	public void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for lab1
		DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
		ArrayList<Page> modifiedPages = databaseFile.insertTuple(tid, t);
		for (Page page : modifiedPages) {
			page.markDirty(true, tid);
			this.pageMap.put(page.getId(), page);
		}
	}

	/**
	 * Remove the specified tuple from the buffer pool. Will acquire a write lock on
	 * the page the tuple is removed from and any other pages that are updated. May
	 * block if the lock(s) cannot be acquired.
	 *
	 * Marks any pages that were dirtied by the operation as dirty by calling their
	 * markDirty bit, and adds versions of any pages that have been dirtied to the
	 * cache (replacing any existing versions of those pages) so that future
	 * requests see up-to-date pages.
	 *
	 * @param tid the transaction deleting the tuple.
	 * @param t   the tuple to delete
	 */
	public void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for lab1
		DbFile databaseFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
		ArrayList<Page> modifiedPages = databaseFile.deleteTuple(tid, t);
		for (Page page : modifiedPages) {
			page.markDirty(true, tid);
			this.pageMap.put(page.getId(), page);
		}
	}

	/**
	 * Flush all dirty pages to disk. NB: Be careful using this routine -- it writes
	 * dirty data to disk so will break simpledb if running in NO STEAL mode.
	 */
	public synchronized void flushAllPages() throws IOException {
		// some code goes here
		// not necessary for lab1
		for (PageId pid : this.pageMap.keySet())
			flushPage(pid);
	}

	/**
	 * Remove the specific page id from the buffer pool. Needed by the recovery
	 * manager to ensure that the buffer pool doesn't keep a rolled back page in its
	 * cache.
	 * 
	 * Also used by B+ tree files to ensure that deleted pages are removed from the
	 * cache so they can be reused safely
	 */
	public synchronized void discardPage(PageId pid) {
		// some code goes here
		// not necessary for lab1

	}

	/**
	 * Flushes a certain page to disk
	 * 
	 * @param pid an ID indicating the page to flush
	 */
	private synchronized void flushPage(PageId pid) throws IOException {
		// some code goes here
		// not necessary for lab1
		Page pageToFlush = this.pageMap.get(pid);
		TransactionId dirtyTransactionId = pageToFlush.isDirty();
		if (dirtyTransactionId != null) {
			DbFile databaseFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
			databaseFile.writePage(pageToFlush);
			pageToFlush.markDirty(false, dirtyTransactionId);
		}
	}

	/**
	 * Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
		for (PageId pid : pageMap.keySet()) {
			Page p = pageMap.get(pid);
			if (p.isDirty() == tid)
				flushPage(pid);

		}
	}

	/**
	 * Discards a page from the buffer pool. Flushes the page to disk to ensure
	 * dirty pages are updated on disk.
	 */
	private synchronized void evictPage() throws DbException {
		// some code goes here
		// not necessary for lab1
		ArrayList<PageId> cleanPages = new ArrayList<PageId>();
		for (PageId pid : pageMap.keySet()) {
			if (pageMap.get(pid).isDirty() == null) {
				cleanPages.add(pid);
			}
		}
		if (cleanPages.size() == 0)
			throw new DbException("No clean pages to evict.");
		PageId vic = cleanPages.get((int) Math.floor(Math.random() * cleanPages.size()));
		try {
			assert pageMap.get(vic).isDirty() == null : "Evicting a dirty page!";
			flushPage(vic);
		} catch (Exception e) {
			e.printStackTrace();
		}
		pageMap.remove(vic);
	}
}
