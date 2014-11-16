package com.algolia.output;

import org.json.JSONObject;

public class Printer extends Output {

	@Override
	public void addObject(JSONObject obj) {
		System.out.println(obj.toString());
	}

	@Override
	public void flush() {
	}

	@Override
	public void close() {
	}

}
