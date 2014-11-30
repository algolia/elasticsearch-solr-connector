package com.algolia;

import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Options;
import org.json.JSONException;
import org.json.JSONObject;

import com.algolia.input.Elasticsearch;
import com.algolia.input.Input;
import com.algolia.input.Solr;
import com.algolia.output.Output;
import com.algolia.output.Printer;
import com.algolia.output.Pusher;

public class Connector 
{
	/*
	 * Variables
	 */
	public static final Logger logger = Logger.getLogger("convert");
	public static final Options options = new Options();
	public static JSONObject configuration = new JSONObject();

	/*
	 * Constants
	 */
	// {NAME, SHORT_NAME, DESC, PARAM}
	public static final String[][] CONF_NAME = { { "params", null, "Parameters for the input.", "" }, {"input", null, "ES or SOLR", ""},
			{ "debug", "d", "Activate the debug mode", null }, { "appID", "u", "The application ID.", "" }, { "apiKey", "p", "The api key.", "" }, { "indexName", "i", "Name of the output index.", ""},
			{ "help", "h", "Print help", null } };

	// Index of parameter in the CONF_NAME variable
	public enum CONF_IDX {
		CONF_PARAM, CONF_INPUT, CONF_DEBUG, CONF_APPID, CONF_APIKEY, CONF_INDEXNAME, CONF_HELP
	}
	
	/*
	 * Helpers
	 */

	static {
		for (String[] param : CONF_NAME) {
			assert (param.length == 3 || param.length == 4);
			options.addOption(param[1], param[0], param.length == 4 && param[3] != null, param[2]);
		}
	}
	
	private static void usage(Exception e) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(160);
		formatter.printHelp("Elasticsearchconnector [option]...", options);
		throw new Error(e);
	}
	
    public static void main( String[] args ) throws JSONException
    {
    	CommandLine cli = null;
		if (CONF_NAME.length != CONF_IDX.values().length) {
			throw new IllegalStateException("CONF_NAME.length != CONF_IDX.values().length");
		}

		logger.info("Parse command-line");
		try {
			cli = new BasicParser().parse(options, args, false);
			String[] unparsedTargets = cli.getArgs();
			if (unparsedTargets.length > 0 || cli.hasOption(CONF_NAME[CONF_IDX.CONF_HELP.ordinal()][0])) {
				usage(new Exception());
			}
		} catch (org.apache.commons.cli.ParseException e) {
		    System.err.println(e.getMessage());
			usage(e);
		}
		
		try {
			if (!cli.hasOption(CONF_NAME[CONF_IDX.CONF_DEBUG.ordinal()][0])) {
				if (!cli.hasOption(CONF_NAME[CONF_IDX.CONF_APIKEY.ordinal()][0])) {
					throw new MissingOptionException(String.format("Missing parameter %s", CONF_NAME[CONF_IDX.CONF_APIKEY.ordinal()][0]));
				}
				if (!cli.hasOption(CONF_NAME[CONF_IDX.CONF_APPID.ordinal()][0])) {
					throw new MissingOptionException(String.format("Missing parameter %s", CONF_NAME[CONF_IDX.CONF_APPID.ordinal()][0]));
				}
			}
			if (!cli.hasOption(CONF_NAME[CONF_IDX.CONF_PARAM.ordinal()][0])) {
				throw new MissingOptionException(String.format("Missing parameter %s", CONF_NAME[CONF_IDX.CONF_PARAM.ordinal()][0]));
			}
			if (!cli.hasOption(CONF_NAME[CONF_IDX.CONF_INPUT.ordinal()][0])) {
				throw new MissingOptionException(String.format("Missing parameter %s", CONF_NAME[CONF_IDX.CONF_INPUT.ordinal()][0]));
			}
		} catch (MissingOptionException e) {
			usage(e);
		}
		
		Output output;
		
		if (cli.hasOption(CONF_NAME[CONF_IDX.CONF_DEBUG.ordinal()][0])) {
			output = new Printer();
		} else {
			output = new Pusher(cli.getOptionValue(CONF_NAME[CONF_IDX.CONF_APPID.ordinal()][0]), cli.getOptionValue(CONF_NAME[CONF_IDX.CONF_APIKEY.ordinal()][0]), cli.getOptionValue(CONF_NAME[CONF_IDX.CONF_INDEXNAME.ordinal()][0]));
		}
		
		logger.info(String.format("Start enumaration %s", cli.getOptionValue(CONF_NAME[CONF_IDX.CONF_PARAM.ordinal()][0])));
		Input enumerator = null;
		switch (cli.getOptionValue(CONF_NAME[CONF_IDX.CONF_INPUT.ordinal()][0])) {
			case "ES":
				enumerator = new Elasticsearch(output, cli.getOptionValue(CONF_NAME[CONF_IDX.CONF_PARAM.ordinal()][0]).split("\\|"));
				break;
			case "SOLR":
				enumerator = new Solr(output, cli.getOptionValue(CONF_NAME[CONF_IDX.CONF_PARAM.ordinal()][0]).split("\\|"));
				break;
			default:
				usage(new MissingOptionException(String.format("Wrong parameter %s", CONF_NAME[CONF_IDX.CONF_INPUT.ordinal()][0])));
		}
		enumerator.enumerate();
		logger.info("End enumaration");
		output.close();
    }
}
