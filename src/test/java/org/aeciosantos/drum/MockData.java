package org.aeciosantos.drum;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import com.aeciosantos.drum.KeyValueStorable;

public class MockData extends KeyValueStorable {
	
	public static final Charset UTF8 = Charset.forName("UTF-8");
	
	public String key;
	public int count;
	
	public MockData() { }
	
	public MockData(String url, int count) {
		this.key = url;
		this.count = count;
	}

	@Override
	public byte[] getKey() {
		return key.getBytes(UTF8);
	}

	@SuppressWarnings("unchecked")
	@Override
	public MockData merge(KeyValueStorable other) {
		MockData obj = (MockData) other;
		assert this.key.equals(obj.key);
		MockData merged = new MockData(this.key, this.count + obj.count);
		System.err.println("merging("+this.key+", "+obj.key+") count="+merged.count);
		return merged;
	}
	
	@Override
	public void readFrom(ByteBuffer buf) {
		int urlSize = buf.getInt();
		byte[] strBuf = new byte[urlSize];
		buf.get(strBuf);
		this.key = new String(strBuf);
		this.count = buf.getInt();
		System.err.println("read: key="+key+" count="+count);
	}
	
	@Override
	public void writeTo(ByteBuffer buf) {
		byte[] bytes = this.key.getBytes(UTF8);
		buf.putInt(bytes.length);
		buf.put(bytes);
		buf.putInt(this.count);
		System.err.println("written: key="+key+" count="+count);
	}
	
}
