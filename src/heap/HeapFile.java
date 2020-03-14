package heap;

import global.GlobalConst;
import global.RID;

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
  //
  int numRecords = 0;
  boolean temporary = false;
  private String name;

  public HeapFile(String name) {
    // call allocate_page
      //PUT YOUR CODE HERE
    // TODO
    this.name = name;
    if(name == null) {
      // make temp HeapFile w/o DB entry
      this.temporary = true;
    } else if(/*name is the name of a file that exists*/) {
      //open file
    } else {
      // create a new empty file
    }
  }

  /**
   * Called by the garbage collector when there are no more references to the
   * object; deletes the heap file if it's temporary.
   */
  protected void finalize() throws Throwable {
      //PUT YOUR CODE HERE
    if(this.temporary) {
      // TODO delete heapFile
    }
  }

  /**
   * Deletes the heap file from the database, freeing all of its pages.
   */
  public void deleteFile() {
    //PUT YOUR CODE HERE
    // TODO free all pages
    // TODO delete heapFile
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
  public Tuple getRecord(RID rid) throws Exception {
    //PUT YOUR CODE HERE
    // if invalid RID, throw IllegalArgumentException
    try {
      if(/*rid is invalid*/) {
        throw new IllegalArgumentException("invalid rid argument");
      }
      // TODO get Tuple from rid
      return null;  // TODO return Tuple, not null
    } catch(IllegalArgumentException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Updates the specified record in the heap file.
   * 
   * @throws IllegalArgumentException if the rid or new record is invalid
   */
  public boolean updateRecord(RID rid, Tuple newRecord) throws Exception {
    //PUT YOUR CODE HERE
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
    //PUT YOUR CODE HERE
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
