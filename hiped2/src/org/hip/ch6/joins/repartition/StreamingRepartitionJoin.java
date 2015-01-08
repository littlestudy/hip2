package org.hip.ch6.joins.repartition;

import org.hip.base.BaseMapReduce;

public class StreamingRepartitionJoin extends BaseMapReduce{	

	public StreamingRepartitionJoin() {
		super(StreamingRepartitionJoin.class);
	}

	public static void main(String[] args) throws Exception {
		exec(new StreamingRepartitionJoin(), args);
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		
		info("begin");
		
		return 0;
	}

}
