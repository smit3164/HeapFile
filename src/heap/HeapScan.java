package heap;

import java.util.*;
import global.* ;
import chainexception.ChainException;

/**
 * A HeapScan object is created only through the function openScan() in the
 * HeapFile class. It supports the getNext interface which will simply retrieve
 * the next record in the file.
 */
public class HeapScan implements GlobalConst {

    HeapFile hf;
    HFPage hfPage;
    RID record;
    ArrayList<PageId> pageIds;
    int pageIdIndex = 0;

    /**
     * Constructs a file scan by pinning the directory header page and initializing
     * iterator fields.
     */
    protected HeapScan(HeapFile hf) {
        this.hf = hf;
        pageIds = hf.pageIds;

        hfPage = new HFPage();

        Minibase.BufferManager.pinPage(pageIds.get(pageIdIndex), hfPage, false);

        record = hfPage.firstRecord();
    }

    /**
     * Called by the garbage collector when there are no more references to the
     * object; closes the scan if it's still open.
     */
    protected void finalize() throws Throwable {
        hf = null;
        hfPage = null;
        record = null;

        close();
    }

    /**
     * Closes the file scan, releasing any pinned pages.
     */
    public void close() throws ChainException{
        Minibase.BufferManager.unpinPage(pageIds.get(pageIdIndex), false);
    }

    /**
     * Returns true if there are more records to scan, false otherwise.
     */
    public boolean hasNext() {
        return pageIdIndex < pageIds.size() - 1;
    }

    /**
     * Gets the next record in the file scan.
     *
     * @param rid output parameter that identifies the returned record
     * @throws IllegalStateException if the scan has no more elements
     */
    public Tuple getNext(RID rid) throws Exception {
        // There is a next record to return
        if (record != null) {
            rid.copyRID(record);
            record = hfPage.nextRecord(record);

            return new Tuple(hfPage.selectRecord(rid), 0, hfPage.selectRecord(rid).length);
        }

        // There is another page of records to use
        if (hasNext()) {
            Minibase.BufferManager.unpinPage(pageIds.get(pageIdIndex++), false);
            Minibase.BufferManager.pinPage(pageIds.get(pageIdIndex), hfPage, false);

            record = hfPage.firstRecord();

            // Found end of records
            if (record == null) {
                Minibase.BufferManager.unpinPage(pageIds.get(pageIdIndex), false);
                return null;
            }

            rid.copyRID(record);
            record = hfPage.nextRecord(record);

            return new Tuple(hfPage.selectRecord(rid), 0, hfPage.selectRecord(rid).length);
        }

        Minibase.BufferManager.unpinPage(pageIds.get(pageIdIndex), false);

        return null;
    }

}
