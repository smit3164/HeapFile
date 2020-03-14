package heap;

import java.util.*;
import global.* ;
import chainexception.ChainException;
import jdk.dynalink.support.ChainedCallSite;

/**
 * A HeapScan object is created only through the function openScan() in the
 * HeapFile class. It supports the getNext interface which will simply retrieve
 * the next record in the file.
 */
public class HeapScan implements GlobalConst {

  /**
   * Constructs a file scan by pinning the directory header page and initializing
   * iterator fields.
   */
  protected HeapScan(HeapFile hf) {
    //PUT YOUR CODE HERE
    
  }

  /**
   * Called by the garbage collector when there are no more references to the
   * object; closes the scan if it's still open.
   */
  protected void finalize() throws Throwable {
    //PUT YOUR CODE HERE
    close();
  }

  /**
   * Closes the file scan, releasing any pinned pages.
   */
  public void close() throws ChainException{
    //PUT YOUR CODE HERE
  }

  /**
   * Returns true if there are more records to scan, false otherwise.
   */
  public boolean hasNext() {
    //PUT YOUR CODE HERE
    return false;
  }

  /**
   * Gets the next record in the file scan.
   * 
   * @param rid output parameter that identifies the returned record
   * @throws IllegalStateException if the scan has no more elements
   */
  public Tuple getNext(RID rid) throws Exception {
    //PUT YOUR CODE HERE
    return null;
  }

} // public class HeapScan implements GlobalConst
