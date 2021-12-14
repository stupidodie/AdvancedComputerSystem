package com.acertainbookstore.client.tests;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

import java.util.*;

import com.acertainbookstore.business.*;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acertainbookstore.client.BookStoreHTTPProxy;
import com.acertainbookstore.client.StockManagerHTTPProxy;
import com.acertainbookstore.interfaces.BookStore;
import com.acertainbookstore.interfaces.StockManager;
import com.acertainbookstore.utils.BookStoreConstants;
import com.acertainbookstore.utils.BookStoreException;

/**
 * {@link BookStoreTest} tests the {@link BookStore} interface.
 * 
 * @see BookStore
 */
public class BookStoreTest {

	/** The Constant TEST_ISBN. */
	private static final int TEST_ISBN = 3044560;

	/** The Constant NUM_COPIES. */
	private static final int NUM_COPIES = 5;

	/** The local test. */
	private static boolean localTest = true;

	/** Single lock test */
	private static boolean singleLock = false;

	
	/** The store manager. */
	private static StockManager storeManager;

	/** The client. */
	private static BookStore client;

	/**
	 * Sets the up before class.
	 */
	@BeforeClass
	public static void setUpBeforeClass() {
		try {
			String localTestProperty = System.getProperty(BookStoreConstants.PROPERTY_KEY_LOCAL_TEST);
			localTest = (localTestProperty != null) ? Boolean.parseBoolean(localTestProperty) : localTest;
			
			String singleLockProperty = System.getProperty(BookStoreConstants.PROPERTY_KEY_SINGLE_LOCK);
			singleLock = (singleLockProperty != null) ? Boolean.parseBoolean(singleLockProperty) : singleLock;

			if (localTest) {
				if (singleLock) {
					SingleLockConcurrentCertainBookStore store = new SingleLockConcurrentCertainBookStore();
					storeManager = store;
					client = store;
				} else {
					TwoLevelLockingConcurrentCertainBookStore store = new TwoLevelLockingConcurrentCertainBookStore();
					storeManager = store;
					client = store;
				}
			} else {
				storeManager = new StockManagerHTTPProxy("http://localhost:8081/stock");
				client = new BookStoreHTTPProxy("http://localhost:8081");
			}

			storeManager.removeAllBooks();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Helper method to add some books.
	 *
	 * @param isbn
	 *            the isbn
	 * @param copies
	 *            the copies
	 * @throws BookStoreException
	 *             the book store exception
	 */
	public void addBooks(int isbn, int copies) throws BookStoreException {
		Set<StockBook> booksToAdd = new HashSet<StockBook>();
		StockBook book = new ImmutableStockBook(isbn, "Test of Thrones", "George RR Testin'", (float) 10, copies, 0, 0,
				0, false);
		booksToAdd.add(book);
		storeManager.addBooks(booksToAdd);
	}

	/**
	 * Helper method to get the default book used by initializeBooks.
	 *
	 * @return the default book
	 */
	public StockBook getDefaultBook() {
		return new ImmutableStockBook(TEST_ISBN, "Harry Potter and JUnit", "JK Unit", (float) 10, NUM_COPIES, 0, 0, 0,
				false);
	}

	/**
	 * Method to add a book, executed before every test case is run.
	 *
	 * @throws BookStoreException
	 *             the book store exception
	 */
	@Before
	public void initializeBooks() throws BookStoreException {
		Set<StockBook> booksToAdd = new HashSet<StockBook>();
		booksToAdd.add(getDefaultBook());
		storeManager.addBooks(booksToAdd);
	}

	/**
	 * Method to clean up the book store, execute after every test case is run.
	 *
	 * @throws BookStoreException
	 *             the book store exception
	 */
	@After
	public void cleanupBooks() throws BookStoreException {
		storeManager.removeAllBooks();
	}

	/**
	 * Tests basic buyBook() functionality.
	 *
	 * @throws BookStoreException
	 *             the book store exception
	 */
	@Test
	public void testBuyAllCopiesDefaultBook() throws BookStoreException {
		// Set of books to buy
		Set<BookCopy> booksToBuy = new HashSet<BookCopy>();
		booksToBuy.add(new BookCopy(TEST_ISBN, NUM_COPIES));

		// Try to buy books
		client.buyBooks(booksToBuy);

		List<StockBook> listBooks = storeManager.getBooks();
		assertTrue(listBooks.size() == 1);
		StockBook bookInList = listBooks.get(0);
		StockBook addedBook = getDefaultBook();

		assertTrue(bookInList.getISBN() == addedBook.getISBN() && bookInList.getTitle().equals(addedBook.getTitle())
				&& bookInList.getAuthor().equals(addedBook.getAuthor()) && bookInList.getPrice() == addedBook.getPrice()
				&& bookInList.getNumSaleMisses() == addedBook.getNumSaleMisses()
				&& bookInList.getAverageRating() == addedBook.getAverageRating()
				&& bookInList.getNumTimesRated() == addedBook.getNumTimesRated()
				&& bookInList.getTotalRating() == addedBook.getTotalRating()
				&& bookInList.isEditorPick() == addedBook.isEditorPick());
	}

	/**
	 * Tests that books with invalid ISBNs cannot be bought.
	 *
	 * @throws BookStoreException
	 *             the book store exception
	 */
	@Test
	public void testBuyInvalidISBN() throws BookStoreException {
		List<StockBook> booksInStorePreTest = storeManager.getBooks();

		// Try to buy a book with invalid ISBN.
		HashSet<BookCopy> booksToBuy = new HashSet<BookCopy>();
		booksToBuy.add(new BookCopy(TEST_ISBN, 1)); // valid
		booksToBuy.add(new BookCopy(-1, 1)); // invalid

		// Try to buy the books.
		try {
			client.buyBooks(booksToBuy);
			fail();
		} catch (BookStoreException ex) {
			;
		}

		List<StockBook> booksInStorePostTest = storeManager.getBooks();

		// Check pre and post state are same.
		assertTrue(booksInStorePreTest.containsAll(booksInStorePostTest)
				&& booksInStorePreTest.size() == booksInStorePostTest.size());
	}

	/**
	 * Tests that books can only be bought if they are in the book store.
	 *
	 * @throws BookStoreException
	 *             the book store exception
	 */
	@Test
	public void testBuyNonExistingISBN() throws BookStoreException {
		List<StockBook> booksInStorePreTest = storeManager.getBooks();

		// Try to buy a book with ISBN which does not exist.
		HashSet<BookCopy> booksToBuy = new HashSet<BookCopy>();
		booksToBuy.add(new BookCopy(TEST_ISBN, 1)); // valid
		booksToBuy.add(new BookCopy(100000, 10)); // invalid

		// Try to buy the books.
		try {
			client.buyBooks(booksToBuy);
			fail();
		} catch (BookStoreException ex) {
			;
		}

		List<StockBook> booksInStorePostTest = storeManager.getBooks();

		// Check pre and post state are same.
		assertTrue(booksInStorePreTest.containsAll(booksInStorePostTest)
				&& booksInStorePreTest.size() == booksInStorePostTest.size());
	}

	/**
	 * Tests that you can't buy more books than there are copies.
	 *
	 * @throws BookStoreException
	 *             the book store exception
	 */
	@Test
	public void testBuyTooManyBooks() throws BookStoreException {
		List<StockBook> booksInStorePreTest = storeManager.getBooks();

		// Try to buy more copies than there are in store.
		HashSet<BookCopy> booksToBuy = new HashSet<BookCopy>();
		booksToBuy.add(new BookCopy(TEST_ISBN, NUM_COPIES + 1));

		try {
			client.buyBooks(booksToBuy);
			fail();
		} catch (BookStoreException ex) {
			;
		}

		List<StockBook> booksInStorePostTest = storeManager.getBooks();
		assertTrue(booksInStorePreTest.containsAll(booksInStorePostTest)
				&& booksInStorePreTest.size() == booksInStorePostTest.size());
	}

	/**
	 * Tests that you can't buy a negative number of books.
	 *
	 * @throws BookStoreException
	 *             the book store exception
	 */
	@Test
	public void testBuyNegativeNumberOfBookCopies() throws BookStoreException {
		List<StockBook> booksInStorePreTest = storeManager.getBooks();

		// Try to buy a negative number of copies.
		HashSet<BookCopy> booksToBuy = new HashSet<BookCopy>();
		booksToBuy.add(new BookCopy(TEST_ISBN, -1));

		try {
			client.buyBooks(booksToBuy);
			fail();
		} catch (BookStoreException ex) {
			;
		}

		List<StockBook> booksInStorePostTest = storeManager.getBooks();
		assertTrue(booksInStorePreTest.containsAll(booksInStorePostTest)
				&& booksInStorePreTest.size() == booksInStorePostTest.size());
	}

	/**
	 * Tests that all books can be retrieved.
	 *
	 * @throws BookStoreException
	 *             the book store exception
	 */
	@Test
	public void testGetBooks() throws BookStoreException {
		Set<StockBook> booksAdded = new HashSet<StockBook>();
		booksAdded.add(getDefaultBook());

		Set<StockBook> booksToAdd = new HashSet<StockBook>();
		booksToAdd.add(new ImmutableStockBook(TEST_ISBN + 1, "The Art of Computer Programming", "Donald Knuth",
				(float) 300, NUM_COPIES, 0, 0, 0, false));
		booksToAdd.add(new ImmutableStockBook(TEST_ISBN + 2, "The C Programming Language",
				"Dennis Ritchie and Brian Kerninghan", (float) 50, NUM_COPIES, 0, 0, 0, false));

		booksAdded.addAll(booksToAdd);

		storeManager.addBooks(booksToAdd);

		// Get books in store.
		List<StockBook> listBooks = storeManager.getBooks();

		// Make sure the lists equal each other.
		assertTrue(listBooks.containsAll(booksAdded) && listBooks.size() == booksAdded.size());
	}

	/**
	 * Tests that a list of books with a certain feature can be retrieved.
	 *
	 * @throws BookStoreException
	 *             the book store exception
	 */
	@Test
	public void testGetCertainBooks() throws BookStoreException {
		Set<StockBook> booksToAdd = new HashSet<StockBook>();
		booksToAdd.add(new ImmutableStockBook(TEST_ISBN + 1, "The Art of Computer Programming", "Donald Knuth",
				(float) 300, NUM_COPIES, 0, 0, 0, false));
		booksToAdd.add(new ImmutableStockBook(TEST_ISBN + 2, "The C Programming Language",
				"Dennis Ritchie and Brian Kerninghan", (float) 50, NUM_COPIES, 0, 0, 0, false));

		storeManager.addBooks(booksToAdd);

		// Get a list of ISBNs to retrieved.
		Set<Integer> isbnList = new HashSet<Integer>();
		isbnList.add(TEST_ISBN + 1);
		isbnList.add(TEST_ISBN + 2);

		// Get books with that ISBN.
		List<Book> books = client.getBooks(isbnList);

		// Make sure the lists equal each other
		assertTrue(books.containsAll(booksToAdd) && books.size() == booksToAdd.size());
	}

	/**
	 * Tests that books cannot be retrieved if ISBN is invalid.
	 *
	 * @throws BookStoreException
	 *             the book store exception
	 */
	@Test
	public void testGetInvalidIsbn() throws BookStoreException {
		List<StockBook> booksInStorePreTest = storeManager.getBooks();

		// Make an invalid ISBN.
		HashSet<Integer> isbnList = new HashSet<Integer>();
		isbnList.add(TEST_ISBN); // valid
		isbnList.add(-1); // invalid

		HashSet<BookCopy> booksToBuy = new HashSet<BookCopy>();
		booksToBuy.add(new BookCopy(TEST_ISBN, -1));

		try {
			client.getBooks(isbnList);
			fail();
		} catch (BookStoreException ex) {
			;
		}

		List<StockBook> booksInStorePostTest = storeManager.getBooks();
		assertTrue(booksInStorePreTest.containsAll(booksInStorePostTest)
				&& booksInStorePreTest.size() == booksInStorePostTest.size());
	}
	private class BuyBooksRunnable implements Runnable{
		private  Set<BookCopy> books;
		private int operationNumbers=0;
		BuyBooksRunnable(int operationNumbers, Set<BookCopy> books){
			this.books=books;
			this.operationNumbers=operationNumbers;
		}
		@Override
		public void run() {
			try{
				for(int i=0;i<operationNumbers;i++){
					client.buyBooks(books);
				}
			}catch (BookStoreException e){
				e.printStackTrace();
			}
		}
	}
	private class AddCopiesRunnable implements Runnable {
		private Set<BookCopy> books;
		private int operationNumbers = 0;

		AddCopiesRunnable(int operationNumbers, Set<BookCopy> books) {
			this.books = books;
			this.operationNumbers = operationNumbers;
		}

		@Override
		public void run() {
			try {
				for (int i = 0; i < operationNumbers; i++) {
					storeManager.addCopies(books);
				}
			} catch (BookStoreException e) {
				e.printStackTrace();
			}
		}
	}
		/** The first test code*?
	 *
	 * @throws BookStoreException
	 */
	@Test
	public void test1() throws BookStoreException{
		storeManager.removeAllBooks();
		final int TEST_NUMBER=3;
		addBooks(TEST_ISBN,NUM_COPIES*100);
		addBooks(TEST_ISBN+1,NUM_COPIES*100);
		addBooks(TEST_ISBN+2,NUM_COPIES*100);
		Set<BookCopy> booksToBuyAndAddCopies = new HashSet<>();
		booksToBuyAndAddCopies.add(new BookCopy(TEST_ISBN,TEST_NUMBER));
		booksToBuyAndAddCopies.add(new BookCopy(TEST_ISBN+1, TEST_NUMBER));
		booksToBuyAndAddCopies.add(new BookCopy(TEST_ISBN+2, TEST_NUMBER));
		Thread t1 =new Thread(new BuyBooksRunnable(100,booksToBuyAndAddCopies));
		Thread t2= new Thread(new AddCopiesRunnable(100,booksToBuyAndAddCopies));
		t1.start();
		t2.start();
		try{
			t1.join();
			t2.join();
		}catch (InterruptedException e){
			e.printStackTrace();
		}
		assertEquals(NUM_COPIES*100,storeManager.getBooksByISBN(new HashSet<>(singletonList(TEST_ISBN))).get(0).getNumCopies());
		assertEquals(NUM_COPIES*100,storeManager.getBooksByISBN(new HashSet<>(singletonList(TEST_ISBN+1))).get(0).getNumCopies());
		assertEquals(NUM_COPIES*100,storeManager.getBooksByISBN(new HashSet<>(singletonList(TEST_ISBN+2))).get(0).getNumCopies());
	}
	private class BuyAndAddCopies implements Runnable{
		private final int operationNumbers;
		private Set<BookCopy> bookCopies;
		BuyAndAddCopies(int operationNumbers,Set<BookCopy>  bookCopies){
			this.bookCopies=bookCopies;
			this.operationNumbers=operationNumbers;
		}

		@Override
		public void run() {
			try{
				for(int i=0;i<operationNumbers;i++){
					client.buyBooks(bookCopies);
					storeManager.addCopies(bookCopies);
				}
			}catch (BookStoreException e){
				e.printStackTrace();
			}
		}
	}
	private static class CheckSnapshots implements Runnable{
		private final int operationNumbers;
		private static boolean error=false;
		CheckSnapshots(int operationNumbers){
			this.operationNumbers=operationNumbers;
		}
		public static boolean getError(){
			return error;
		}
		@Override
		public void run(){
				for(int i=0;i<operationNumbers;i++){
					List<StockBook> books=new ArrayList<>();
					try{
						books=storeManager.getBooks();
					}catch (BookStoreException e){
						error=true;
						e.printStackTrace();
					}
					for(StockBook b: books){
						if (!(b.getNumCopies() == NUM_COPIES || b.getNumCopies() == 0)) {
							error = true;
						}
					}

				}
			}
	}
	@Test
	public void test2() throws BookStoreException{

		addBooks(TEST_ISBN+1,NUM_COPIES);
		addBooks(TEST_ISBN+2,NUM_COPIES);
		Set<BookCopy> booksToBuyAndAdd = new HashSet<>();
		booksToBuyAndAdd.add(new BookCopy(TEST_ISBN,NUM_COPIES));
		booksToBuyAndAdd.add(new BookCopy(TEST_ISBN+1, NUM_COPIES));
		booksToBuyAndAdd.add(new BookCopy(TEST_ISBN+2, NUM_COPIES));

		Thread t1 = new Thread(new BuyAndAddCopies(100,booksToBuyAndAdd));
		Thread t2 = new Thread(new CheckSnapshots(100));
		t1.start();
		t2.start();
		try {
			t1.join();
			t2.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		assertFalse( CheckSnapshots.getError());
	}

	private class BuyAndAddCopies2 implements Runnable{
		private final int operationNumbers;
		private Set<BookCopy> bookCopies1;
		private Set<BookCopy> bookCopies2;
		BuyAndAddCopies2(int operationNumbers,Set<BookCopy>  bookCopies1,Set<BookCopy>  bookCopies2){
			this.bookCopies1=bookCopies1;
			this.bookCopies2=bookCopies2;
			this.operationNumbers=operationNumbers;
		}

		@Override
		public void run() {
			try{
				for(int i=0;i<operationNumbers;i++){
					client.buyBooks(bookCopies1);
					storeManager.addCopies(bookCopies2);
				}
			}catch (BookStoreException e){
				e.printStackTrace();
			}
		}
	}
	@Test
	public void test3() throws BookStoreException{
		storeManager.removeAllBooks();
		addBooks(TEST_ISBN,NUM_COPIES*100);
		addBooks(TEST_ISBN+1,NUM_COPIES*100);
		addBooks(TEST_ISBN+2,NUM_COPIES*100);
		Set<BookCopy> booksToBuy = new HashSet<>();
		Set<BookCopy> booksToAdd = new HashSet<>();
		booksToBuy.add(new BookCopy(TEST_ISBN,NUM_COPIES));
		booksToAdd.add(new BookCopy(TEST_ISBN,1));
		booksToBuy.add(new BookCopy(TEST_ISBN+1, NUM_COPIES));
		booksToAdd.add(new BookCopy(TEST_ISBN+1, 1));
		booksToBuy.add(new BookCopy(TEST_ISBN+2, NUM_COPIES));
		booksToAdd.add(new BookCopy(TEST_ISBN+2, 1));
		Thread t1 = new Thread(new BuyAndAddCopies2(10,booksToBuy,booksToAdd));
		Thread t2 =new Thread(new BuyAndAddCopies2(9,booksToAdd,booksToBuy));
		t1.start();
		t2.start();
		try{
			t1.join();
			t2.join();
		}catch (InterruptedException e){
			e.printStackTrace();
		}
		assertEquals(NUM_COPIES*100-4,storeManager.getBooksByISBN(new HashSet<>(singletonList(TEST_ISBN))).get(0).getNumCopies());
		assertEquals(NUM_COPIES*100-4,storeManager.getBooksByISBN(new HashSet<>(singletonList(TEST_ISBN+1))).get(0).getNumCopies());
		assertEquals(NUM_COPIES*100-4,storeManager.getBooksByISBN(new HashSet<>(singletonList(TEST_ISBN+2))).get(0).getNumCopies());

	}
	private class UpdateEditorPicker implements Runnable{
		private final int operationNumbers;
		Set<BookEditorPick> editorPicks1;
		Set<BookEditorPick> editorPicks2;
		UpdateEditorPicker(int operationNumbers,Set<BookEditorPick> editorPicks1,Set<BookEditorPick> editorPicks2){
			this.operationNumbers=operationNumbers;
			this.editorPicks1=editorPicks1;
			this.editorPicks2=editorPicks2;
		}
		@Override
		public void run(){
			try{
				for(int i=0;i<operationNumbers;i++){
					storeManager.updateEditorPicks(editorPicks1);
					storeManager.updateEditorPicks(editorPicks2);
				}
			}catch (BookStoreException e){
				e.printStackTrace();
			}
		}
	}
	private static class CheckSnapshots2 implements Runnable{
		private final int operationNumbers;
		private static boolean error=false;
		CheckSnapshots2(int operationNumbers){
			this.operationNumbers=operationNumbers;
		}
		public static boolean getError(){
			return error;
		}
		@Override
		public void run(){
			for(int i=0;i<operationNumbers;i++){
				List<Book> books=new ArrayList<>();
				try{
					books=client.getEditorPicks(1);
				}catch (BookStoreException e){
					error=true;
					e.printStackTrace();
				}
				if (!(books.get(0).getISBN() == TEST_ISBN || books.get(0).getISBN() == TEST_ISBN+1)) {
						error = true;
					}

			}
		}
	}
	@Test
	public void test4() throws BookStoreException{

		addBooks(TEST_ISBN+1,NUM_COPIES);
		Set<BookEditorPick> editorPicks1=new HashSet<>();
		Set<BookEditorPick> editorPicks2=new HashSet<>();
		editorPicks1.add(new BookEditorPick(TEST_ISBN,true));
		editorPicks1.add(new BookEditorPick(TEST_ISBN+1,false));
		storeManager.updateEditorPicks(editorPicks1);
		editorPicks1.add(new BookEditorPick(TEST_ISBN,false));
		editorPicks1.add(new BookEditorPick(TEST_ISBN+1,true));
		Thread t1= new Thread(new UpdateEditorPicker(20,editorPicks1,editorPicks2));
		Thread t2=new Thread(new CheckSnapshots2(20));
		t1.start();
		t2.start();
		try {
			t1.join();
			t2.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		assertFalse( CheckSnapshots.getError());


	}
	/**
	 * Tear down after class.
	 *
	 * @throws BookStoreException
	 *             the book store exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws BookStoreException {
		storeManager.removeAllBooks();

		if (!localTest) {
			((BookStoreHTTPProxy) client).stop();
			((StockManagerHTTPProxy) storeManager).stop();
		}
	}
}
