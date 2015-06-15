package com.aeciosantos.drum;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;

public class BucketIterator<Data extends KeyValueStorable> implements Iterator<Data>, Closeable {

	private RandomAccessFile randomAccessFile;
	private FileChannel fileChannel;
	private MappedByteBuffer mappedByteBuffer;
	private Class<Data> clazz;
	private boolean reachedEof;

	public BucketIterator(Class<Data> clazz, String fileName) throws IOException {
		this.clazz = clazz;
		this.randomAccessFile = new RandomAccessFile(fileName, "r");
		this.fileChannel = randomAccessFile.getChannel();
		this.mappedByteBuffer = fileChannel.map(MapMode.READ_ONLY, 0, fileChannel.size());
		verifyEndOfFile();
	}

	@Override
	public boolean hasNext() {
		if(!reachedEof) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public Data next() {
		Data instance;
		try {
			instance = clazz.newInstance();
		} catch (Exception e) {
			throw new IllegalStateException("Class "+clazz+" should have a public constructor without parameters!");
		}
		instance.readFrom(mappedByteBuffer);
		verifyEndOfFile();
		return instance;
	}

	private void verifyEndOfFile() {
		if(mappedByteBuffer.position() < mappedByteBuffer.capacity()) {
			reachedEof = false;
		} else {
			reachedEof = true;
			close();
		}
	}

	@Override
	public void close() {
		try {
			randomAccessFile.close();
			fileChannel.close();
		} catch (IOException e) {
			System.err.println("Failed to close bucket file. " + e);
		}
	}
	
	@Override
	public void remove() {
        throw new UnsupportedOperationException("remove() is not supported yet.");
    }
	
	@Override
	protected void finalize() throws Throwable {
		if(!reachedEof) {
			close();
		}
		super.finalize();
	}

}
