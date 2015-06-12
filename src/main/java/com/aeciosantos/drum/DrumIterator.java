package com.aeciosantos.drum;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;

public class DrumIterator<Data extends KeyValueStorable> implements Iterator<Data>, Closeable {

	private RandomAccessFile randomAccessFile;
	private FileChannel fileChannel;
	private MappedByteBuffer mappedByteBuffer;
	private Class<Data> clazz;

	public DrumIterator(Class<Data> clazz, String fileName) throws IOException {
		this.clazz = clazz;
		this.randomAccessFile = new RandomAccessFile(fileName, "r");
		this.fileChannel = randomAccessFile.getChannel();
		this.mappedByteBuffer = fileChannel.map(MapMode.READ_ONLY, 0, fileChannel.size());
	}

	@Override
	public boolean hasNext() {
		if(mappedByteBuffer.position() < mappedByteBuffer.capacity()) {
			System.err.println("hasNext=true");
			return true;
		} else {
			System.err.println("hasNext=false");
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
		System.err.println("readKey="+new String(instance.getKey()));
		return instance;
	}

	@Override
	public void close() throws IOException {
		randomAccessFile.close();
	}
	
	@Override
	public void remove() {
        throw new UnsupportedOperationException("remove() is not supported yet.");
    }

}
