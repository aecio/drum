package org.aeciosantos.drum;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aeciosantos.drum.BucketIterator;
import com.aeciosantos.drum.DiskRepositoryMerge;

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
		
		DiskRepositoryMerge<MockData> drum = new DiskRepositoryMerge<MockData>(MockData.class, fileName);
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
		BucketIterator<MockData> iterator = drum.getIterator();
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

}
