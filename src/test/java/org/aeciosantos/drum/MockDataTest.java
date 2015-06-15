package org.aeciosantos.drum;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.nio.ByteBuffer;

import org.junit.Test;

public class MockDataTest {

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
