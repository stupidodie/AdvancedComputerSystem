package com.acertainbookstore.client.workloads;

import java.util.*;
import java.util.stream.Collectors;

import com.acertainbookstore.business.ImmutableStockBook;
import com.acertainbookstore.business.StockBook;

/**
 * Helper class to generate stockbooks and isbns modelled similar to Random
 * class
 */
public class BookSetGenerator {

    private final Random random;
	private final int maxIsbn=99999999;

    public BookSetGenerator() {
        // TODO Auto-generated constructor stub
        this.random = new Random();

    }

    public BookSetGenerator(long seed) {
        this.random = new Random(seed);
    }

    /**
     * Returns num randomly selected isbns from the input set
     *
     * @param num
	 * number of the size
     * @return
     */
    public Set<Integer> sampleFromSetOfISBNs(Set<Integer> isbns, int num) {
        final List<Integer> isbnsList = new ArrayList<>(isbns);
        Collections.shuffle(isbnsList);
        return isbnsList.stream().limit(num).collect(Collectors.toSet());
    }

    /**
     * Return num stock books. For now return an ImmutableStockBook
     *
     * @param num
	 * number of the size
     * @return
     */
    public Set<StockBook> nextSetOfStockBooks(int num) {
		Set<Integer> randomSet= generateRandomSet(this.random,num);
        Set<StockBook> bookSet= new HashSet<>();
		randomSet.forEach(isbn->bookSet.add(createBook(isbn)));
		return bookSet;
    }
	private Set<Integer> generateRandomSet(Random random, int size){
		HashSet<Integer> randomSet = new HashSet<>();
		do {
			int tempIsbn = random.nextInt(this.maxIsbn) + 1;
			randomSet.add(tempIsbn);
		} while (randomSet.size() != size);
		return randomSet;
	}

    private ImmutableStockBook createBook(int isbn) {
        Random random = new Random(isbn);
        final int maxNumber = 10;
        final float priceScale = 200;
        final int maxMiss = 20;
        final int maxRateNum = 30;
        final int maxRating = 5;
		final float price=random.nextFloat() * priceScale + 10;
        return new ImmutableStockBook(
                isbn, getRandomString(),
                getRandomString(),
                price,
                random.nextInt(maxNumber)+1,
				random.nextInt(maxMiss),
				random.nextInt(maxRateNum),
				random.nextInt(maxRating),
                random.nextBoolean()
        );

    }

    private static String getRandomString() {
        String stringSet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        final int stringSetlength = stringSet.length();
        final int maxLength = 10;
        Random random = new Random();
        StringBuilder stringBuffer = new StringBuilder();
        for (int i = 0; i < maxLength; i++) {
            int number = random.nextInt(stringSetlength);
            stringBuffer.append(stringSet.charAt(number));
        }
        return stringBuffer.toString();
    }

}
