package com.aeciosantos.drum;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Bucket<Data extends KeyValueStorable> {
	
	private Class<Data> clazz;
	private int maxBucketSize;
	private int bucketId;
	private String fileName;
	private List<Data> items;
	
	public Bucket(Class<Data> clazz, int maxBucketSize, int bucketId, String baseFileName) throws IOException {
		this.clazz = clazz;
		this.maxBucketSize = maxBucketSize;
		this.items = new ArrayList<Data>();
		this.bucketId = bucketId;
		this.fileName = String.format("%s.%d", baseFileName, bucketId);;
		createFileIfDoestExist(fileName);
	}
	
	private void createFileIfDoestExist(String fileName) throws IOException {
		File file = new File(fileName);
		if(!file.exists()) {
			file.createNewFile();
		}
	}

	public void add(Data data) {
		items.add(data);
	}

	public List<Data> getItems() {
		return items;
	}

	public boolean reachedMaxSize() {
		return items.size() >= maxBucketSize;
	}

	public int size() {
		return items.size();
	}
	
	public int getBucketId() {
		return bucketId;
	}

	public String getFilename() {
		return fileName;
	}

	public BucketIterator<Data> getDiskIterator() throws IOException {
		return new BucketIterator<Data>(clazz, getFilename());
	}

}
