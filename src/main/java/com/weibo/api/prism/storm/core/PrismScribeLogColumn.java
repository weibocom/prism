package com.weibo.api.prism.storm.core;

public enum PrismScribeLogColumn {
	BYTES("fields.prism.scribe.logs.bytes"),
	IP("fields.prism.scribe.log.ip"),
	UNIX_TIME("fields.prism.scribe.log.unix-time"),
	REQUEST_ID("fields.prism.scribe.log.req-id"),
	MODULE("fields.prism.scribe.log.module"),
	LOG_TYPE("fields.prism.scribe.log.log-type"),
	DATA_TYPE("fields.prism.scribe.log.data-type"),
	DATA("fields.prism.scribe.log.data");
	
	
	
	private String val;
	
	private PrismScribeLogColumn(String v){
		this.val = v;
	}
	
	public String value(){
		return val;
	}
}
