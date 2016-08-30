package com.test.ymd.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {
	
	public SimplePartitioner (VerifiableProperties props) {
	}
	
	public int partition(Object key, int a_numPartitions) {
		int partition = 0;
		String partitionKey = (String)key;
		int offset = partitionKey.lastIndexOf(".");
		if(offset > 0) {
			partition = Integer.valueOf(partitionKey.substring(offset+1)) % a_numPartitions;
		}
		return partition;
	}

}
