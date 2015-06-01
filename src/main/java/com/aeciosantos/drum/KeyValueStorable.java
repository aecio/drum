package com.aeciosantos.drum;

import java.nio.ByteBuffer;

public abstract class KeyValueStorable implements Comparable<KeyValueStorable> {
	
	public abstract byte[] getKey();
	
	public abstract <Data extends KeyValueStorable> Data merge(Data other);
	
	public abstract void writeTo(ByteBuffer buf);

	public abstract void readFrom(ByteBuffer buf);
	
	public int compareTo(KeyValueStorable o) {
		return compare(this.getKey(), o.getKey());
	}
	
	// TODO maybe substitute by Google Guava's comparator
	public static int compare(byte[] left, byte[] right) {
		for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
			int a = (left[i] & 0xff);
			int b = (right[j] & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return left.length - right.length;
	}
	
	@SuppressWarnings("unchecked")
    public static <Data extends KeyValueStorable> Data[] merge(Data[] toAdd) {
        if (toAdd.length == 1) {
            return toAdd;
        }
        // estimate the number of unique elements and check the precondition,
        // that the array must been sorted
        int count = 1;
        for (int i = 0; i < toAdd.length - 1; i++) {
            int compare = compare(toAdd[i].getKey(), toAdd[i + 1].getKey());
            if (compare > 0) {
                throw new RuntimeException("The given array is not sorted.");
            } else if(compare < 0) {
                count++;
            } else { // if compare == 0
                // do nothing, elements are equal
            }
        }

        KeyValueStorable[] realToAdd = new KeyValueStorable[count];
        count = 0;
        // merge Elements in toAdd
        KeyValueStorable first = toAdd[0];
        for (int k = 1; k < toAdd.length; k++) {
            while (k < toAdd.length && compare(toAdd[k].getKey(), first.getKey()) == 0) {
                first = first.merge(toAdd[k]);
                k++;
            }
            realToAdd[count++] = first;
            if (k < toAdd.length) {
                first = toAdd[k];
            }
        }

        if (count < realToAdd.length && compare(realToAdd[realToAdd.length - 2].getKey(), first.getKey()) != 0) {
            realToAdd[count++] = first;
        }
        return (Data[]) realToAdd;
    }


}
