package com.aeciosantos.drum;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.aeciosantos.drum.hash.HashFunction;

public class VEUNIQ<Data extends KeyValueStorable> {

	private static final int MAX_BUCKET_SIZE = 1000;
	
	private final Class<Data> clazz;
	private final String fileName;
	private Bucket<Data>[] buckets;
	private HashFunction<Data> hashFunction;
	
	public VEUNIQ(Class<Data> clazz, String fileName, int numberOfBuckets) throws IOException {
		this.clazz = clazz;
		this.fileName = fileName;
		checkClassIsInstantiable(clazz);
		createBuckets(numberOfBuckets);
		hashFunction = new HashFunction<Data>(numberOfBuckets);
	}
	
	private void checkClassIsInstantiable(Class<Data> clazz) {
		try {
			clazz.newInstance();
		} catch (Exception e) {
			throw new IllegalArgumentException("Class " + clazz
					+ " should have a public constructor without parameters!");
		}
	}

	@SuppressWarnings("unchecked")
	private void createBuckets(int numberOfBuckets) throws IOException {
		buckets = new Bucket[numberOfBuckets];
		for (int bucketId = 0; bucketId < numberOfBuckets; bucketId++) {
			buckets[bucketId] = new Bucket<Data>(clazz, MAX_BUCKET_SIZE, bucketId, fileName);
		}
	}

	public void insertOrMerge(Data data) throws IOException {
		int bucketId = hashFunction.getBucketIdFor(data);
		Bucket<Data> bucket = buckets[bucketId];
		bucket.add(data);
		if(bucket.reachedMaxSize()) {
			syncronize(bucketId);
		}
	}
	
	public void syncronize(int bucketId) throws IOException {
		Bucket<Data> bucket = buckets[bucketId]; 
		Syncronizer<Data> syncronizer = new Syncronizer<Data>(bucket.getItems(), bucket.getDiskIterator());
		syncronizer.run(bucket.getFilename());
		buckets[bucketId] = new Bucket<Data>(clazz, MAX_BUCKET_SIZE, bucketId, fileName); 
	}

	public void syncronizeAll() throws IOException {
		for (int bucketId = 0; bucketId < buckets.length; bucketId++) {
			syncronize(bucketId);
		}
	}
	
	public VEUNIQIterator<Data> getIterator() throws IOException {
		return new VEUNIQIterator<Data>(buckets);
	}
	
	public static class VEUNIQIterator<Data extends KeyValueStorable> implements Iterator<Data> {
		
		private Iterator<Data> current;
		private Iterator<Iterator<Data>> cursor;
		
		public VEUNIQIterator(Bucket<Data>[] buckets) throws IOException {
			if (buckets == null) {
				throw new IllegalArgumentException("iterators is null");
			}
			List<Iterator<Data>> iterators = new ArrayList<Iterator<Data>>();
			for (Bucket<Data> bucket : buckets) {
				iterators.add(bucket.getDiskIterator());
			}
			this.cursor = iterators.iterator();
		}

		private Iterator<Data> findNext() {
	        while (cursor.hasNext()) {
	            current = cursor.next();
	            if (current.hasNext()) return current;
	        }
	        return null;
	    }
		
		@Override
		public boolean hasNext() {
			if (current == null || !current.hasNext()) {
				current = findNext();
			}
			return (current != null && current.hasNext());
		}

		@Override
		public Data next() {
			return current.next();
		}
		
	}
	
	public class Iterators<T> implements Iterator<T> {
	 
	    private Iterator<T> current;
	    private Iterator<Iterator<T>> cursor;
	 
	    public Iterators(Iterable<Iterator<T>> iterators) {
	        if (iterators == null) throw new IllegalArgumentException("iterators is null");
	        this.cursor = iterators.iterator();
	    }
	 
	    private Iterator<T> findNext() {
	        while (cursor.hasNext()) {
	            current = cursor.next();
	            if (current.hasNext()) return current;
	        }
	        return null;
	    }
	 
	    @Override
	    public boolean hasNext() {
	        if (current == null || !current.hasNext()) {
	            current = findNext();
	        }
	        return (current != null && current.hasNext());
	    }
	 
	    @Override
	    public T next() {
	        return current.next();
	    }
	 
	    @Override
	    public void remove() {
	        if (current != null) {
	            current.remove();
	        }
	    }
	}

}
