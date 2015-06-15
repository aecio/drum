package com.aeciosantos.drum.hash;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.aeciosantos.drum.KeyValueStorable;

public class HashFunction<Data extends KeyValueStorable> {
	
	private int numberOfBuckets;

	public HashFunction(int numberOfBuckets) {
		this.numberOfBuckets = numberOfBuckets;
	}

	public int getBucketIdFor(Data data) {
//		return md5hash(data);
		return murmur32hash(data);
	}

	private int murmur32hash(Data data) {
		int hash32 = MurmurHash.hash32(data.getKey(), data.getKey().length);
		return Math.abs(hash32) % numberOfBuckets;
	}

	@SuppressWarnings("unused")
	private int md5hash(Data data) {
		MessageDigest md5;
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			String message = "Failed to instantiate MD5 message digest.";
			throw new IllegalStateException(message, e);
		}
		byte[] digest = md5.digest(data.getKey());
		BigInteger hashValue = new BigInteger(1, digest);
		BigInteger bucketId = hashValue.mod(BigInteger.valueOf(numberOfBuckets));
		return bucketId.intValue();
	}
	
}
