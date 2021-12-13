package com.acertainbookstore.business;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


import com.acertainbookstore.interfaces.BookStore;
import com.acertainbookstore.interfaces.StockManager;
import com.acertainbookstore.utils.BookStoreConstants;
import com.acertainbookstore.utils.BookStoreException;
import com.acertainbookstore.utils.BookStoreUtility;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
/**
 * {@link TwoLevelLockingConcurrentCertainBookStore} implements the {@link BookStore} and
 * {@link StockManager} functionalities.
 *
 * @see BookStore
 * @see StockManager
 */
public class TwoLevelLockingConcurrentCertainBookStore implements BookStore, StockManager {

    /**
     * The mapping of books from ISBN to {@link BookStoreBook}.
     */
    private Map<Integer, BookStoreBook> bookMap = null;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock globalShareLock = readWriteLock.readLock();
    private final Lock globalExclusiveLock = readWriteLock.writeLock();

    /**
     * Use ConcurrentMap instead of Map due to the Concurrency control
     */
    private ConcurrentMap<Integer, ReadWriteLock> lockMap = null;

	private boolean addGlobalExclusiveLock() throws BookStoreException {
		boolean success =false;
		try {
			success=globalExclusiveLock.tryLock(1, TimeUnit.SECONDS);
		}catch (InterruptedException e){

		}
		if(!success){
			throw new BookStoreException("Add Global Exclusive Lock Fail!");
		}
		return success;
	}
    private boolean putLocalLock(int ISBN) throws BookStoreException {
        if(!validateISBNNotInLockMap(ISBN)){
			return false;
		}
        lockMap.put(ISBN, new ReentrantReadWriteLock());
		return true;
    }

    private boolean addLocalShareLock(int ISBN) throws BookStoreException {
        boolean globalLock=false, localLock=false, success;
        if(!validateISBNInLockMap(ISBN)){
			return false;
		}
        ReadWriteLock readWriteLock = lockMap.get(ISBN);
		try {
			globalLock = globalShareLock.tryLock(1, TimeUnit.SECONDS);
			localLock = readWriteLock.readLock().tryLock(1, TimeUnit.SECONDS);
		}catch (InterruptedException e){

		}
        success = globalLock && localLock;
        if (success) {
            return success;
        } else if (globalLock) {
            globalShareLock.unlock();
            return success;
        } else if (localLock) {
            readWriteLock.readLock().unlock();
            return success;
        } else {
            return success;
        }
    }

    private boolean addLocalExclusiveLock(int ISBN) throws BookStoreException {
        boolean globalLock=false, localLock=false, success;
		if(!validateISBNInLockMap(ISBN)){
			return false;
		}
        ReadWriteLock readWriteLock = lockMap.get(ISBN);
		try {
			globalLock = globalShareLock.tryLock(1, TimeUnit.SECONDS);
			localLock = readWriteLock.writeLock().tryLock(1, TimeUnit.SECONDS);
		}catch (InterruptedException e){

		}
        success = globalLock && localLock;
        if (success) {
            return success;
        } else if (globalLock) {
            globalShareLock.unlock();
            return success;
        } else if (localLock) {
            readWriteLock.writeLock().unlock();
            return success;
        } else {
            return success;
        }
    }

    private void releaseLocalShareLock(int ISBN) {
        ReadWriteLock readWriteLock = lockMap.get(ISBN);
        globalShareLock.unlock();
        readWriteLock.readLock().unlock();
    }

    private void releaseLocalExclusiveLock(int ISBN) {
        ReadWriteLock readWriteLock = lockMap.get(ISBN);
        globalShareLock.unlock();
        readWriteLock.writeLock().unlock();
    }

    /**
     * Instantiates a new {@link CertainBookStore}.
     */
    public TwoLevelLockingConcurrentCertainBookStore() {
        // Constructors are not synchronized
        bookMap = new HashMap<>();
        lockMap = new ConcurrentHashMap<>();
    }

    private void validate(StockBook book) throws BookStoreException {
        int isbn = book.getISBN();
        String bookTitle = book.getTitle();
        String bookAuthor = book.getAuthor();
        int noCopies = book.getNumCopies();
        float bookPrice = book.getPrice();

        if (BookStoreUtility.isInvalidISBN(isbn)) { // Check if the book has valid ISBN
            throw new BookStoreException(BookStoreConstants.ISBN + isbn + BookStoreConstants.INVALID);
        }

        if (BookStoreUtility.isEmpty(bookTitle)) { // Check if the book has valid title
            throw new BookStoreException(BookStoreConstants.BOOK + book.toString() + BookStoreConstants.INVALID);
        }

        if (BookStoreUtility.isEmpty(bookAuthor)) { // Check if the book has valid author
            throw new BookStoreException(BookStoreConstants.BOOK + book.toString() + BookStoreConstants.INVALID);
        }

        if (BookStoreUtility.isInvalidNoCopies(noCopies)) { // Check if the book has at least one copy
            throw new BookStoreException(BookStoreConstants.BOOK + book.toString() + BookStoreConstants.INVALID);
        }

        if (bookPrice < 0.0) { // Check if the price of the book is valid
            throw new BookStoreException(BookStoreConstants.BOOK + book.toString() + BookStoreConstants.INVALID);
        }

        if (bookMap.containsKey(isbn)) {// Check if the book is not in stock
            throw new BookStoreException(BookStoreConstants.ISBN + isbn + BookStoreConstants.DUPLICATED);
        }
    }

    private void validate(BookCopy bookCopy) throws BookStoreException {
        int isbn = bookCopy.getISBN();
        int numCopies = bookCopy.getNumCopies();

        validateISBNInStock(isbn); // Check if the book has valid ISBN and in stock

        if (BookStoreUtility.isInvalidNoCopies(numCopies)) { // Check if the number of the book copy is larger than zero
            throw new BookStoreException(BookStoreConstants.NUM_COPIES + numCopies + BookStoreConstants.INVALID);
        }
    }

    private void validate(BookEditorPick editorPickArg) throws BookStoreException {
        int isbn = editorPickArg.getISBN();
        validateISBNInStock(isbn); // Check if the book has valid ISBN and in stock
    }

    private void validateISBNInStock(Integer ISBN) throws BookStoreException {
        if (BookStoreUtility.isInvalidISBN(ISBN)) { // Check if the book has valid ISBN
            throw new BookStoreException(BookStoreConstants.ISBN + ISBN + BookStoreConstants.INVALID);
        }
        if (!bookMap.containsKey(ISBN)) {// Check if the book is in stock
            throw new BookStoreException(BookStoreConstants.ISBN + ISBN + BookStoreConstants.NOT_AVAILABLE);
        }
    }

    private boolean validateISBNInLockMap(Integer ISBN)  {
        if (!lockMap.containsKey(ISBN)) {
           return false;
        }
		return true;
    }

    private boolean validateISBNNotInLockMap(Integer ISBN) {
        if (lockMap.containsKey(ISBN)) {
           return false;
        }
		return true;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.acertainbookstore.interfaces.StockManager#addBooks(java.util.Set)
     */
    public void addBooks(Set<StockBook> bookSet) throws BookStoreException {
        if (bookSet == null) {
            throw new BookStoreException(BookStoreConstants.NULL_INPUT);
        }
		boolean success=false;
        try {

			success=addGlobalExclusiveLock();
            // Check if all are there
            for (StockBook book : bookSet) {
                validate(book);
            }
			List<Integer> isbnList= new ArrayList<>();
            for (StockBook book : bookSet) {
                int isbn = book.getISBN();

                boolean successAdd=putLocalLock(isbn);
				if(successAdd){
					isbnList.add(isbn);
				}else{
					for (Integer Isbn: isbnList){
						lockMap.remove(Isbn);
					}
					throw new BookStoreException("Error");
				}
            }
			for(StockBook book:bookSet){
				int isbn = book.getISBN();
				bookMap.put(isbn, new BookStoreBook(book));
			}
			globalExclusiveLock.unlock();
        } catch (BookStoreException e){
			if(success){
				globalExclusiveLock.unlock();
			}
			throw e;
		}
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.acertainbookstore.interfaces.StockManager#addCopies(java.util.Set)
     */
    public void addCopies(Set<BookCopy> bookCopiesSet) throws BookStoreException {
        int isbn;
        int numCopies;

        if (bookCopiesSet == null) {
            throw new BookStoreException(BookStoreConstants.NULL_INPUT);
        }
        try {
			List<Integer> isbnLockList= new ArrayList<>();
            for (BookCopy bookCopy : bookCopiesSet) {
				boolean success = addLocalExclusiveLock(bookCopy.getISBN());
				try {
					validate(bookCopy);
				} catch (BookStoreException e){
					if(success) {
						releaseLocalExclusiveLock(bookCopy.getISBN());
					}
					success = false;
				}
				if(success){
					isbnLockList.add(bookCopy.getISBN());
				}else{
					for(Integer ISBN: isbnLockList){
						releaseLocalExclusiveLock(ISBN);
					}
					throw new BookStoreException("Add Local Exclusive Lock failed "+ bookCopy.getISBN());
				}
            }
            BookStoreBook book;
            // Update the number of copies
            for (BookCopy bookCopy : bookCopiesSet) {
                isbn = bookCopy.getISBN();
                numCopies = bookCopy.getNumCopies();
                book = bookMap.get(isbn);
                book.addCopies(numCopies);
				releaseLocalExclusiveLock(isbn);
            }
        } catch (BookStoreException e) {
            throw e;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see com.acertainbookstore.interfaces.StockManager#getBooks()
     */
    public List<StockBook> getBooks() throws BookStoreException{
		try {
			Collection<BookStoreBook> bookMapValues = bookMap.values();
			List<Integer> isbnLockList= new ArrayList<>();
			for (BookStoreBook book : bookMapValues) {
				boolean success = addLocalShareLock(book.getISBN());
				if(success){
					isbnLockList.add(book.getISBN());
				}else{
					for(Integer ISBN: isbnLockList){
						releaseLocalShareLock(ISBN);
					}
					throw new BookStoreException("Add Local Share Lock failed "+ book.getISBN());
				}
			}

			List<StockBook> result= bookMapValues.stream()
					.map(book -> book.immutableStockBook())
					.collect(Collectors.toList());
			for (BookStoreBook book : bookMapValues) {
				releaseLocalShareLock(book.getISBN());
			}
			return result;
		}catch (BookStoreException e){
			throw e;
		}
	}

    /*
     * (non-Javadoc)
     *
     * @see
     * com.acertainbookstore.interfaces.StockManager#updateEditorPicks(java.util
     * .Set)
     */
    public void updateEditorPicks(Set<BookEditorPick> editorPicks) throws BookStoreException {
		// Check that all ISBNs that we add/remove are there first.
		if (editorPicks == null) {
			throw new BookStoreException(BookStoreConstants.NULL_INPUT);
		}
		try {
			List<Integer> isbnLockList= new ArrayList<>();
			for (BookEditorPick editorPickArg : editorPicks) {
				boolean success = addLocalShareLock(editorPickArg.getISBN());
				try {
					validate(editorPickArg);
				} catch (BookStoreException e){
					if(success){
						releaseLocalShareLock(editorPickArg.getISBN());
					}
					success = false;
				}
				if(success){
					isbnLockList.add(editorPickArg.getISBN());
				}else{
					for(Integer ISBN: isbnLockList){
						releaseLocalShareLock(ISBN);
					}
					throw new BookStoreException("Add Local Share Lock failed "+ editorPickArg.getISBN());
				}

			}

			for (BookEditorPick editorPickArg : editorPicks) {
				bookMap.get(editorPickArg.getISBN()).setEditorPick(editorPickArg.isEditorPick());
				releaseLocalShareLock(editorPickArg.getISBN());
			}
		}catch (BookStoreException e){
			throw e;
		}
	}

    /*
     * (non-Javadoc)
     *
     * @see com.acertainbookstore.interfaces.BookStore#buyBooks(java.util.Set)
     */
    public void buyBooks(Set<BookCopy> bookCopiesToBuy) throws BookStoreException {
		if (bookCopiesToBuy == null) {
			throw new BookStoreException(BookStoreConstants.NULL_INPUT);
		}
		try {
			// Check that all ISBNs that we buy are there first.
			int isbn;
			BookStoreBook book;
			Boolean saleMiss = false;

			Map<Integer, Integer> salesMisses = new HashMap<>();
			List<Integer> isbnLockList= new ArrayList<>();
			for (BookCopy bookCopyToBuy : bookCopiesToBuy) {
				isbn = bookCopyToBuy.getISBN();
				boolean success = addLocalShareLock(isbn);
				try {
					validate(bookCopyToBuy);
				} catch (BookStoreException e){
					if(success) {
						releaseLocalShareLock(isbn);
					}
					success = false;
				}
				if(success){
					isbnLockList.add(isbn);
				}else{
					for(Integer ISBN: isbnLockList){
						releaseLocalShareLock(ISBN);
					}
					throw new BookStoreException("Add Local Share Lock failed "+ isbn);
				}

				book = bookMap.get(isbn);

				if (!book.areCopiesInStore(bookCopyToBuy.getNumCopies())) {
					// If we cannot sell the copies of the book, it is a miss.
					salesMisses.put(isbn, bookCopyToBuy.getNumCopies() - book.getNumCopies());
					saleMiss = true;
				}
			}

			// We throw exception now since we want to see how many books in the
			// order incurred misses which is used by books in demand
			if (saleMiss) {
				for (Map.Entry<Integer, Integer> saleMissEntry : salesMisses.entrySet()) {
					book = bookMap.get(saleMissEntry.getKey());
					book.addSaleMiss(saleMissEntry.getValue());
				}
				for(BookCopy bookCopyToBuy:bookCopiesToBuy){
					releaseLocalShareLock(bookCopyToBuy.getISBN());
				}
				throw new BookStoreException(BookStoreConstants.BOOK + BookStoreConstants.NOT_AVAILABLE);
			}

			// Then make the purchase.
			for (BookCopy bookCopyToBuy : bookCopiesToBuy) {
				book = bookMap.get(bookCopyToBuy.getISBN());
				book.buyCopies(bookCopyToBuy.getNumCopies());
				releaseLocalShareLock(bookCopyToBuy.getISBN());
			}
		} catch (BookStoreException e){
			throw e;
		}
	}

    /*
     * (non-Javadoc)
     *
     * @see
     * com.acertainbookstore.interfaces.StockManager#getBooksByISBN(java.util.
     * Set)
     */
    public List<StockBook> getBooksByISBN(Set<Integer> isbnSet) throws BookStoreException {
		if (isbnSet == null) {
			throw new BookStoreException(BookStoreConstants.NULL_INPUT);
		}
		try {
			List<Integer> isbnLockList= new ArrayList<>();
			for (Integer ISBN : isbnSet) {
				boolean success = addLocalShareLock(ISBN);
				try {
					validateISBNInStock(ISBN);
				} catch (BookStoreException e){
					if(success){
						releaseLocalShareLock(ISBN);
					}
					success = false;
				}
				if(success){
					isbnLockList.add(ISBN);
				}else{
					for(Integer isbn: isbnLockList){
						releaseLocalShareLock(isbn);
					}
					throw new BookStoreException("Add Local Share Lock failed "+ ISBN);
				}
			}
			List<StockBook> result= isbnSet.stream()
					.map(isbn -> bookMap.get(isbn).immutableStockBook())
					.collect(Collectors.toList());
			for(Integer ISBN :isbnSet){
				releaseLocalShareLock(ISBN);
			}
			return result;
		} catch (BookStoreException e){
			throw e;
		}
	}

    /*
     * (non-Javadoc)
     *
     * @see com.acertainbookstore.interfaces.BookStore#getBooks(java.util.Set)
     */
    public List<Book> getBooks(Set<Integer> isbnSet) throws BookStoreException {
		if (isbnSet == null) {
			throw new BookStoreException(BookStoreConstants.NULL_INPUT);
		}
		try {
			List<Integer> isbnLockList = new ArrayList<>();
			// Check that all ISBNs that we rate are there to start with.
			for (Integer ISBN : isbnSet) {
				boolean success = addLocalShareLock(ISBN);
				try {
					validateISBNInStock(ISBN);
				} catch (BookStoreException e){
					if(success){
						releaseLocalShareLock(ISBN);
					}
					success = false;
				}
				if (success) {
					isbnLockList.add(ISBN);
				} else {
					for (Integer isbn : isbnLockList) {
						releaseLocalShareLock(isbn);
					}
					throw new BookStoreException("Add Local Share Lock failed " + ISBN);
				}
			}
			List<Book> result = isbnSet.stream()
					.map(isbn -> bookMap.get(isbn).immutableBook())
					.collect(Collectors.toList());
			for (Integer ISBN : isbnSet) {
				releaseLocalShareLock(ISBN);
			}
			return result;
		} catch (BookStoreException e) {
			throw e;
		}
	}

    /*
     * (non-Javadoc)
     *
     * @see com.acertainbookstore.interfaces.BookStore#getEditorPicks(int)
     */
    public List<Book> getEditorPicks(int numBooks) throws BookStoreException {
		if (numBooks < 0) {
			throw new BookStoreException("numBooks = " + numBooks + ", but it must be positive");
		}
		try {
			List<Integer> isbnLockList = new ArrayList<>();
			List<BookStoreBook> listAllEditorPicks = bookMap.entrySet().stream()
					.map(pair -> pair.getValue())
					.filter(book -> book.isEditorPick())
					.collect(Collectors.toList());
			for (Book book : listAllEditorPicks) {
				boolean success = addLocalShareLock(book.getISBN());
				if (success) {
					isbnLockList.add(book.getISBN());
				} else {
					for (Integer isbn : isbnLockList) {
						releaseLocalShareLock(isbn);
					}
					throw new BookStoreException("Add Local Share Lock failed " + book.getISBN());
				}
			}
			// Find numBooks random indices of books that will be picked.
			Random rand = new Random();
			Set<Integer> tobePicked = new HashSet<>();
			int rangePicks = listAllEditorPicks.size();

			if (rangePicks <= numBooks) {

				// We need to add all books.
				for (int i = 0; i < listAllEditorPicks.size(); i++) {
					tobePicked.add(i);
				}
			} else {

				// We need to pick randomly the books that need to be returned.
				int randNum;

				while (tobePicked.size() < numBooks) {
					randNum = rand.nextInt(rangePicks);
					tobePicked.add(randNum);
				}
			}

			// Return all the books by the randomly chosen indices.
			List<Book> result= tobePicked.stream()
					.map(index -> listAllEditorPicks.get(index).immutableBook())
					.collect(Collectors.toList());
			for(Book book: listAllEditorPicks){
				releaseLocalShareLock(book.getISBN());
			}
			return result;
		} catch (BookStoreException e){
			throw e;
		}
	}

    /*
     * (non-Javadoc)
     *
     * @see com.acertainbookstore.interfaces.BookStore#getTopRatedBooks(int)
     */
    @Override
    public List<Book> getTopRatedBooks(int numBooks) throws BookStoreException {
        throw new BookStoreException();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.acertainbookstore.interfaces.StockManager#getBooksInDemand()
     */
    @Override
    public List<StockBook> getBooksInDemand() throws BookStoreException {
        throw new BookStoreException();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.acertainbookstore.interfaces.BookStore#rateBooks(java.util.Set)
     */
    @Override
    public void rateBooks(Set<BookRating> bookRating) throws BookStoreException {
        throw new BookStoreException();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.acertainbookstore.interfaces.StockManager#removeAllBooks()
     */
    public void removeAllBooks() throws BookStoreException {
		boolean success=false;
		try {
			success=addGlobalExclusiveLock();
			bookMap.clear();
			lockMap.clear();
			globalExclusiveLock.unlock();
		} catch (BookStoreException e){
			if(success){
				globalExclusiveLock.unlock();
			}
			throw e;
		}
	}

    /*
     * (non-Javadoc)
     *
     * @see
     * com.acertainbookstore.interfaces.StockManager#removeBooks(java.util.Set)
     */
    public void removeBooks(Set<Integer> isbnSet) throws BookStoreException {
		if (isbnSet == null) {
			throw new BookStoreException(BookStoreConstants.NULL_INPUT);
		}
		boolean success=false;
		try {
			success=addGlobalExclusiveLock();
			for (Integer ISBN : isbnSet) {
				if (BookStoreUtility.isInvalidISBN(ISBN)) {
					throw new BookStoreException(BookStoreConstants.ISBN + ISBN + BookStoreConstants.INVALID);
				}

				if (!bookMap.containsKey(ISBN)) {
					throw new BookStoreException(BookStoreConstants.ISBN + ISBN + BookStoreConstants.NOT_AVAILABLE);
				}
			}

			for (int isbn : isbnSet) {
				bookMap.remove(isbn);
				lockMap.remove(isbn);
			}
			globalExclusiveLock.unlock();
		} catch (BookStoreException e) {
			if(success){
				globalExclusiveLock.unlock();
			}
			throw e;
		}
	}

}
