package com.aeciosantos.drum;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DRUM<Data extends KeyValueStorable> {
	
	private static final int MAX_ITEMS_IN_BUFFER = 1000;
	
	// FIXME: change by synchronized list
	private List<Data> buffer = new ArrayList<Data>(MAX_ITEMS_IN_BUFFER);

	private Class<Data> clazz;
	private String fileName;
	
	public DRUM(Class<Data> clazz, String fileName) throws IOException {
		this.clazz = clazz;
		this.fileName = fileName;
		checkClassIsInstantiable(clazz);
		checkFile(fileName);
	}

	private void checkFile(String fileName) throws IOException {
		File file = new File(fileName);
		if(!file.exists()) {
			file.createNewFile();
		}
	}

	private void checkClassIsInstantiable(Class<Data> clazz) {
		try {
			clazz.newInstance();
		} catch (Exception e) {
			throw new IllegalArgumentException("Class "+clazz+" "
					+ "should have a public constructor without parameters!");
		}
	}

	public void insertOrMerge(Data data) throws IOException {
		buffer.add(data);
		maybeSyncronize();
	}
	
	private void maybeSyncronize() throws IOException {
		synchronized (buffer) {
			// start an synchronization if necessary
			if(buffer.size() >= MAX_ITEMS_IN_BUFFER) {
				syncronize();
			}
		}
	}
	
	public void syncronize() throws IOException {
		Syncronizer<Data> syncronizer = new Syncronizer<Data>(buffer, getIterator());
		syncronizer.run(fileName);
		buffer = new ArrayList<Data>(MAX_ITEMS_IN_BUFFER);
	}

	public DrumIterator<Data> getIterator() throws IOException {
		return new DrumIterator<Data>(clazz, this.fileName);
	}

}
