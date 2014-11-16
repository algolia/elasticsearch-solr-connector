package com.algolia.output;

import org.json.JSONObject;

public abstract class Output {

	public abstract void addObject(JSONObject obj);
	public abstract void flush();
	public abstract void close();
}
