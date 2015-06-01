package org.aeciosantos.drum;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aeciosantos.drum.DRUM;
import com.aeciosantos.drum.DrumIterator;

public class DRUMTest {
	
	String fileName = "urls.db";

	@Before
	public void setUp() throws Exception {
		File file = new File(fileName);
		if(file.exists()) {
			file.delete();
		}
	}

	@After
	public void tearDown() throws Exception {
	}
	
	

	@Test
	public void shouldMergeDuplicatedItems() throws IOException {
		
		DRUM<MockData> drum = new DRUM<MockData>(MockData.class, fileName);
		MockData url1 = new MockData("asdf", 1);
		MockData url2 = new MockData("qwer", 1);
		
		System.out.println("inser1");
		drum.insertOrMerge(url1);
		drum.insertOrMerge(url1);
		drum.insertOrMerge(url2);
		
		System.out.println("sync");
		drum.syncronize();
		
		System.out.println("insert2");
		drum.insertOrMerge(url1);
		drum.insertOrMerge(url2);
		
		System.out.println("sync2");
		drum.syncronize();
		
		System.out.println("Asserting...");
		
		int count = 0;
		DrumIterator<MockData> iterator = drum.getIterator();
		Map<String, Integer> keyValues = new HashMap<String, Integer>();
		while(iterator.hasNext()) {
			MockData url = iterator.next();
			keyValues.put(url.key, url.count);
			count++;
		}
		
		assertThat(count, is(2));
		
		assertThat(keyValues.get("asdf"), is(notNullValue()));
		assertThat(keyValues.get("asdf"), is(3));
		
		assertThat(keyValues.get("qwer"), is(notNullValue()));
		assertThat(keyValues.get("qwer"), is(2));
		
	}
	
	@Test
	public void mockSerialization() {
		MockData url = new MockData("asdf", 3);
		ByteBuffer buf = ByteBuffer.allocate(1000);
		
		// when
		url.writeTo(buf);
		buf.flip();
		
		MockData readUrl = new MockData();
		readUrl.readFrom(buf);
		
		// then
		assertThat(readUrl.key, is(url.key));
		assertThat(readUrl.count, is(url.count));
	}

}
