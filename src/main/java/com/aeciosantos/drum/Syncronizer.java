package com.aeciosantos.drum;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class Syncronizer<Data extends KeyValueStorable> {

	private List<Data> cacheBuffer;
	private BucketIterator<Data> repositoryIterator;
	
	private ByteBuffer tempBuffer = ByteBuffer.allocate(1024*10);
	
	public Syncronizer(List<Data> cacheBuffer, BucketIterator<Data> repository) throws IOException {
		this.cacheBuffer = cacheBuffer;
		this.repositoryIterator = repository;
	}

	@SuppressWarnings("unchecked")
	public void run(String outputFilename) throws IOException {
		if(cacheBuffer.size() == 0) {
			// no need to synchronize
			return;
		}
		
		// objects should be sorted in order to be merged 
		Collections.sort(cacheBuffer);
		
		// FIXME
		cacheBuffer = (List<Data>) Arrays.asList(KeyValueStorable.merge((KeyValueStorable[]) cacheBuffer.toArray(new KeyValueStorable[cacheBuffer.size()])));
		
		// open output file
		RandomAccessFile randomAccessFile = new RandomAccessFile(outputFilename+".sync", "rw");
		FileChannel fileChannel = randomAccessFile.getChannel();
		
		Iterator<Data> bufferIterator = cacheBuffer.iterator();
		
		// merge objects from disk and buffer, until one ends
		while(this.repositoryIterator.hasNext() && bufferIterator.hasNext()) {
			Data diskHead = repositoryIterator.next();
			Data bufferHead = bufferIterator.next();
			
			int compareTo = diskHead.compareTo(bufferHead);
			if(compareTo == 0) {
				Data merged = diskHead.merge(bufferHead);
				writeObject(fileChannel, merged);
			} else if(compareTo < 0) {
				writeObject(fileChannel, diskHead);
			} else {
				writeObject(fileChannel, bufferHead);
			}
		}
		
		// write reaming objects from disk, if exists
		while(this.repositoryIterator.hasNext()) {
			Data diskHead = repositoryIterator.next();
			writeObject(fileChannel, diskHead);
		}
		
		// write reaming objects from buffer, if exists
		while(bufferIterator.hasNext()) {
			Data bufferHead = bufferIterator.next();
			writeObject(fileChannel, bufferHead);
		}
		flushBufferToFile(fileChannel);
//		fileChannel.force(true);
		
		// close resources
		fileChannel.close();
		randomAccessFile.close();
		
		File orig = new File(outputFilename);
		if(orig.exists()) {
			orig.delete();
		}
		new File(outputFilename+".sync").renameTo(orig);
		
	}

	private void writeObject(FileChannel fileChannel, Data merged) throws IOException {
		int positionBeforeWrite  = tempBuffer.position();
		try {
			merged.writeTo(tempBuffer);
		}
		catch (BufferOverflowException e) {
			// buffer is full, so flush to disk and try to serialize again
			tempBuffer.position(positionBeforeWrite);
			flushBufferToFile(fileChannel);
			merged.writeTo(tempBuffer);
		}
	}

	private void flushBufferToFile(FileChannel fileChannel) throws IOException {
		tempBuffer.flip();
		while(tempBuffer.hasRemaining()) {
		    fileChannel.write(tempBuffer);
		}
		tempBuffer.clear();
	}

}
