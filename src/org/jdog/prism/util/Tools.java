package org.jdog.prism.util;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Tools {
	public static HashMap<String, String> getParams(String[] appParams) {
		HashMap<String, String> paramStore = new HashMap<String, String>();
		if (appParams.length > 0) {
			for (int i = 0; i < appParams.length; i++) {
				Pattern paramPattern = Pattern.compile("^\\--?.*");
				Matcher paramMatcher = paramPattern.matcher(appParams[i]);
				boolean paramMatch = paramMatcher.find();

				if (paramMatch) {
					String value = "1";
					String key = appParams[i];
					key = key.replaceAll("^-+", "");
					i++;

					if (i < appParams.length) {
						paramPattern = Pattern.compile("^\\--?.*");
						paramMatcher = paramPattern.matcher(appParams[i]);
						paramMatch = paramMatcher.find();

						if (paramMatch) {
							i--;
						} else {
							value = appParams[i];
						}
					}
					paramStore.put(key, value);
				}
			}
		}
		return paramStore;
	}

	public static void showHelp() {
		System.out.println();
		System.out
				.println(Variables.APPHELP
						+ "\n\n"
						+ "\t--config <file> Use specific config file, defaults to config.xml\n"
						+ "\t--quiet         Run the application in quiet mode - no output\n"
						+ "\t--verbose       Enable verbose logging\n"
						+ "\t--debug         Enable debug logging\n");
	}

	public static void showBanner() {
		System.out
				.print("\n _____        _        ____                  _____                  \n");
		System.out
				.print("|  __ \\      | |      |  _ \\                / ____|                 \n");
		System.out
				.print("| |  | | __ _| |_ __ _| |_) | __ _ ___  ___| (___  _   _ _ __   ___ \n");
		System.out
				.print("| |  | |/ _` | __/ _` |  _ < / _` / __|/ _ \\\\___ \\| | | | '_ \\ / __|\n");
		System.out
				.print("| |__| | (_| | || (_| | |_) | (_| \\__ \\  __/____) | |_| | | | | (__ \n");
		System.out
				.print("|_____/ \\__,_|\\__\\__,_|____/ \\__,_|___/\\___|_____/ \\__, |_| |_|\\___|\n");
		System.out.print(" " + Variables.APP + " v" + Variables.VERSION
				+ "                         __/ |           \n");
		System.out.print(" " + Variables.AUTHOR
				+ "               |___/            \n");
		System.out.println();
	}
}
