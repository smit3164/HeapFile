package heap;

import chainexception.ChainException;
import global.*;

import java.util.ArrayList;

/**
 * <h3>Minibase Heap Files</h3>
 * A heap file is an unordered set of records, stored on a set of pages. This
 * class provides basic support for inserting, selecting, updating, and deleting
 * records. Temporary heap files are used for external sorting and in other
 * relational operators. A sequential scan of a heap file (via the Scan class)
 * is the most basic access method.
 */
public class HeapFile implements GlobalConst {

    /**
     * If the given name already denotes a file, this opens it; otherwise, this
     * creates a new empty file. A null name produces a temporary heap file which
     * requires no DB entry.
     */

    int numRecords;
    boolean temporary;
    private String name;
    private PageId fPageId;
    private Page fPage;
    private HFPage hfPage;
    private ArrayList<PageId> pageIds;

    public HeapFile(String name) throws ChainException {
        this.name = name;
        this.numRecords = 0;
        this.temporary = false;
        this.fPage = new Page();
        this.hfPage = new HFPage();
        pageIds = new ArrayList<>();

        // Create a new page if no filename is given
        if(name == null) {
            this.temporary = true;
            this.fPageId = new PageId();

            try {
                this.fPageId = Minibase.BufferManager.newPage(this.fPage, 1);
            } catch (Exception e) {
                throw new ChainException(null, "HeapFile.HeapFile: Failed to allocate a new page");
            }

            // Add allocated page to heap file
            this.hfPage.setCurPage(fPageId);

            // Add pageId to array
            this.pageIds.add(this.fPageId);

            return;
        }

        this.fPageId = Minibase.DiskManager.get_file_entry(name);

        // If file doesn't exist, make a new (temporary?) one
        if (fPageId == null) {
            this.temporary = true;
            this.fPageId = Minibase.BufferManager.newPage(this.fPage, 1);

            // Use the given name since it already doesn't exist
            Minibase.DiskManager.add_file_entry(this.name, this.fPageId);

            // Add pageId to array
            this.pageIds.add(this.fPageId);

            // Add allocated page to heap file
            this.hfPage.setCurPage(this.fPageId);

            Minibase.BufferManager.unpinPage(this.fPageId, false);

            return;
        }

        // Pin to associate fPage
        Minibase.BufferManager.pinPage(this.fPageId, this.fPage, false);

        // Add pageId to array
        this.pageIds.add(this.fPageId);

        // Add allocated page to heap file
        this.hfPage.setCurPage(this.fPageId);

        Minibase.BufferManager.unpinPage(this.fPageId, false);
    }

    /**
     * Called by the garbage collector when there are no more references to the
     * object; deletes the heap file if it's temporary.
     */
    protected void finalize() throws Throwable {
        // TODO delete heapFile
        if (temporary) {
            deleteFile();
        }
    }

    /**
     * Deletes the heap file from the database, freeing all of its pages.
     */
    public void deleteFile() throws ChainException {
        for (int i = 0; i < pageIds.size(); i++) {
            try {
                Minibase.BufferManager.freePage(pageIds.get(i));
            } catch (Exception e) {
                throw new ChainException(null, "HeapFile.deleteFile: Failed to free page");
            }
        }

        numRecords = 0;
        pageIds.clear();
        hfPage = null;

        Minibase.DiskManager.delete_file_entry(this.name);
    }

    /**
     * Inserts a new record into the file and returns its RID.
     *
     * @throws IllegalArgumentException if the record is too large
     */
    public RID insertRecord(byte[] record) throws Exception {
        if(record.length > MAX_TUPSIZE) {
            throw new IllegalArgumentException("HeapFile.insertRecord: Argument 'record' is larger than MAX_TUPSIZE");
        }

        // Check each page
        for (int pid = 0; pid < pageIds.size(); pid++) {
            Page page = new Page();
            Minibase.BufferManager.pinPage(pageIds.get(pid), page, false);

            HFPage hf = new HFPage(page);

            if (hf.getFreeSpace() > record.length) {
                RID r = this.hfPage.insertRecord(record);
                this.numRecords++;
                Minibase.BufferManager.unpinPage(pageIds.get(pid), true);
                return r;
            }

            Minibase.BufferManager.unpinPage(pageIds.get(pid), true);
        }

        Page page = new Page();
        PageId pageId = Minibase.BufferManager.newPage(page, 1);
        HFPage hf = new HFPage(page);

        Minibase.BufferManager.pinPage(pageId, page, false);

        pageIds.add(pageId);

        // Add links between the current and new page
        hf.setPrevPage(this.hfPage.getCurPage());
        this.hfPage.setNextPage(pageId);
        this.hfPage = hf;

        // Add record to new page
        RID r = this.hfPage.insertRecord(record);

        Minibase.BufferManager.unpinPage(pageId, false);

        return r;
    }

    /**
     * Reads a record from the file, given its id.
     *
     * @throws IllegalArgumentException if the rid is invalid
     */
    public Tuple getRecord(RID rid) throws ChainException {
        if (!pageIds.contains(rid.pageno)) {
            throw new IllegalArgumentException("HeapFile.deleteRecord: Invalid RID");
        }

        Page page = new Page();
        HFPage hf = new HFPage();
        byte[] record;
        short offset;
        short length;

        try {
            Minibase.BufferManager.pinPage(rid.pageno, page, false);

            hf.copyPage(page);

            record = hf.selectRecord(rid);
            offset = hf.getSlotOffset(rid.slotno);
            length = hf.checkRID(rid);

            Minibase.BufferManager.unpinPage(rid.pageno, false);
        } catch(Exception e) {
            throw new ChainException(null, "HeapFile.getRecord: Failed to retrieve record from HFPage");
        }

        return new Tuple(record, offset, length);
    }

    /**
     * Updates the specified record in the heap file.
     *
     * @throws IllegalArgumentException if the rid or new record is invalid
     */
    public boolean updateRecord(RID rid, Tuple newRecord) throws ChainException {
        if (!pageIds.contains(rid.pageno)) {
            throw new IllegalArgumentException("HeapFile.deleteRecord: Invalid RID");
        }

        Page page = new Page();
        HFPage hf = new HFPage();

        try {
            Minibase.BufferManager.pinPage(rid.pageno, page, false);

            hf.copyPage(page);
            hf.updateRecord(rid, newRecord);

            Minibase.BufferManager.unpinPage(rid.pageno, true);
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    /**
     * Deletes the specified record from the heap file.
     *
     * @throws IllegalArgumentException if the rid is invalid
     */
    public boolean deleteRecord(RID rid) throws IllegalArgumentException {
        if (!pageIds.contains(rid.pageno)) {
            throw new IllegalArgumentException("HeapFile.deleteRecord: Invalid RID");
        }

        Page page = new Page();
        HFPage hf = new HFPage();

        try {
            Minibase.BufferManager.pinPage(rid.pageno, page, false);

            hf.copyPage(page);
            hf.deleteRecord(rid);

            Minibase.BufferManager.unpinPage(rid.pageno, true);
            numRecords--;
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Gets the number of records in the file.
     */
    public int getRecCnt() {
        return this.numRecords;
    }

    /**
     * Initiates a sequential scan of the heap file.
     */
    public HeapScan openScan() {
        return new HeapScan(this);
    }

    /**
     * Returns the name of the heap file.
     */
    public String toString() {
        return this.name;
    }

}
