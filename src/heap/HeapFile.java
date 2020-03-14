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
    boolean temporary = false;
    private String name;
    private PageId fPageId;
    private Page fPage;
    private HFPage hfPage;

    private ArrayList<Page> pages;
    private ArrayList<PageId> pageIds;

    public HeapFile(String name) {
        this.name = name;
        this.hfPage = new HFPage();
        pages = new ArrayList<>();
        pageIds = new ArrayList<>();

        if(name == null) {
            this.temporary = true;
            this.fPageId = new PageId();
            this.fPage = new Page();

            try {
                this.fPageId = Minibase.BufferManager.newPage(this.fPage, 1);
            } catch (Exception e) {
                // TODO: Verify this is correct
            }

            // Add allocated page to heap file
            this.hfPage.setCurPage(fPageId);
        } else {
            // create a new empty file
            this.numRecords = 0;
            this.name = name;

            this.fPageId = Minibase.DiskManager.get_file_entry(name);

            // If file doesn't exist, make a new (temporary?) one
            if (fPageId == null) {
                this.temporary = true;
                this.fPageId = new PageId();
                this.fPage = new Page();

                // Use the given name since it already doesn't exist
                Minibase.DiskManager.add_file_entry(this.name, this.fPageId);

                // TODO: Check if this behavior is correct
                Minibase.DiskManager.read_page(this.fPageId, this.fPage);
            }

            // Add page and pageId to arrays
            this.pages.add(this.fPage);
            this.pageIds.add(this.fPageId);

            // Add allocated page to heap file
            this.hfPage.setCurPage(this.fPageId);
        }
    }

    /**
     * Called by the garbage collector when there are no more references to the
     * object; deletes the heap file if it's temporary.
     */
    protected void finalize() throws Throwable {
        // TODO delete heapFile
    }

    /**
     * Deletes the heap file from the database, freeing all of its pages.
     */
    public void deleteFile() throws ChainException {
        // TODO: free all pages

        for (int i = 0; i < pageIds.size(); i++) {
            try {
                Minibase.BufferManager.freePage(pageIds.get(i));
            } catch (Exception e) {
                throw new ChainException(null, "HeapFile.deleteFile: Failed to free page");
            }
        }

        numRecords = 0;
        pages.clear();
        pageIds.clear();

        // Delete heap file itself?
    }

    /**
     * Inserts a new record into the file and returns its RID.
     *
     * @throws IllegalArgumentException if the record is too large
     */
    public RID insertRecord(byte[] record) throws Exception {
        //PUT YOUR CODE HERE
        // if record is too large, throw IllegalArgumentException
        // TODO create Tuple from byte array
        try {
            if(record.length > MAX_TUPSIZE) {
                // TODO not sure if MAX_TUPSIZE is the correct thing to compare to
                throw new IllegalArgumentException("argument 'record' is larger than MAX_TUPSIZE");
            }
            // TODO add Tuple to heapFile
            this.numRecords++;
            return null;  // TODO return rid, not null
        } catch(IllegalArgumentException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Reads a record from the file, given its id.
     *
     * @throws IllegalArgumentException if the rid is invalid
     */
    public Tuple getRecord(RID rid) throws ChainException {
        byte[] record;
        short offset;
        short length;

        // TODO: Check arguments

        try {
            record = this.hfPage.selectRecord(rid);
            offset = this.hfPage.getSlotOffset(rid.slotno);
            length = this.hfPage.checkRID(rid);
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
        // TODO: Check arguments

        try {
            if(/*rid is invalid*/) {
                throw new IllegalArgumentException("invalid rid argument");
            }
            if (/*newRecord is invalid*/) {
                throw new IllegalArgumentException("invalid newRecord argument");
            }
            Tuple tuple = getRecord(rid);
            // TODO update record
            return true;
        } catch(IllegalArgumentException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Deletes the specified record from the heap file.
     *
     * @throws IllegalArgumentException if the rid is invalid
     */
    public boolean deleteRecord(RID rid) throws IllegalArgumentException {
        // TODO: Check arguments

        try {
            //if RID is invalid, throw IllegalArgumentException
            if(/*rid is invalid*/) {
                throw new IllegalArgumentException("invalid rid argument");
            }
            // TODO delete record from heapFile
            this.numRecords--;
            return true;
        } catch(IllegalArgumentException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Gets the number of records in the file.
     */
    public int getRecCnt() {
        //PUT YOUR CODE HERE
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
        //PUT YOUR CODE HERE
        return this.name;
    }

} // public class HeapFile implements GlobalConst
