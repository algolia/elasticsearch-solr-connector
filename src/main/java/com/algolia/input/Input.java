package com.algolia.input;

import org.json.JSONException;
import org.json.JSONObject;

import com.algolia.output.Output;

public abstract class Input {
	private Output output;
	
	public Input(Output output) {
		this.output = output;
	}
	
	public abstract void enumerate();
	public void addObject(String id, JSONObject obj) {
		try {
			obj.put("objectID", id);
		} catch (JSONException e) {
			throw new Error(e);
		}
		output.addObject(obj);
	}
	public void close() {
		output.close();
	}
}
