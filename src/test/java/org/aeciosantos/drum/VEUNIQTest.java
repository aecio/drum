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

import com.aeciosantos.drum.VEUNIQ;
import com.aeciosantos.drum.VEUNIQ.VEUNIQIterator;

public class VEUNIQTest {
	
	String fileName = "urls.db";
	int numberOfBuckets = 3;

	@Before
	public void setUp() throws Exception {
		for (int i = 0; i < numberOfBuckets; i++) {
			File file = new File(String.format("%s.%d", fileName, i));
			if(file.exists()) {
				file.delete();
			}
		}
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void shouldMergeDuplicatedItems() throws IOException {
		
		VEUNIQ<MockData> veuniq = new VEUNIQ<MockData>(MockData.class, fileName, numberOfBuckets);
		MockData url1 = new MockData("asdf", 1);
		MockData url2 = new MockData("qwer", 1);
		
		System.err.println("---- insert1 ----");
		veuniq.insertOrMerge(url1);
		veuniq.insertOrMerge(url1);
		veuniq.insertOrMerge(url2);
		
		System.err.println("---- sync1 ----");
		veuniq.syncronizeAll();
		
		System.err.println("---- insert1 ----");
		veuniq.insertOrMerge(url1);
		veuniq.insertOrMerge(url2);
		
		System.err.println("---- sync2 ----");
		veuniq.syncronizeAll();
		
		System.err.println("---- Asserting... ----");
		
		int count = 0;
		VEUNIQIterator<MockData> iterator = veuniq.getIterator();
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
