/************************************
 * Main.java
 *
 * Application execution class
 *
 * Written by: Ted Elwartowski
 ************************************/
package org.jdog.prism;

import java.util.HashMap;

import org.jdog.prism.handler.LogHandler;
import org.jdog.prism.handler.RunHandler;
import org.jdog.prism.util.Tools;
import org.jdog.xml.xmlConfig;

/**
 * 
 * @author Ted Elwartowski
 */
public class App {

	public static void main(String[] appParams) {
		boolean hasError = false;
		HashMap<String, String> params = Tools.getParams(appParams);

		xmlConfig config = new xmlConfig();
		LogHandler log = new LogHandler();

		if (params.get("quiet") != null) {
			log.setQuiet(true);
		}

		if (params.get("debug") != null) {
			log.setDebug(true);
		}

		if (params.get("verbose") != null) {
			log.setVerbose(true);
		}

		if (params.get("config") != null) {
			config.setFile((String) params.get("config"));
		}

		try {
			config.loadXML();
		} catch (Exception e) {
			System.out.println(e.toString());
			hasError = true;
		}

		if (!hasError) {
			try {
				if (config.getString("files.log") != null) {
					log.setLogFile(config.getString("files.log"));
				}
			} catch (Exception e) {
				log.log(e.toString());
				if (config.getString("app.errors[@stop]") != null) {
					if (config.getString("app.errors[@stop]").equalsIgnoreCase(
							"yes")) {
						hasError = true;
					}
				}
			}
		}

		if (!hasError) {
			if (config.getString("app.logging[@debug]") != null) {
				if (config.getString("app.logging[@debug]").equalsIgnoreCase(
						"yes")) {
					log.setDebug(true);
				}
			}

			if (config.getString("app.logging[@verbose]") != null) {
				if (config.getString("app.logging[@verbose]").equalsIgnoreCase(
						"yes")) {
					log.setVerbose(true);
				}
			}

			if (!log.isQuiet()) {
				Tools.showBanner();
			}

			RunHandler app = new RunHandler(log, config);
			app.run();
		} else {
			Tools.showHelp();
		}

		try {
			if (log != null) {
				log.close();
			}
		} catch (Exception e) {
			System.out.println(e.toString());
			hasError = true;
		}

		if (hasError) {
			System.exit(1);
		}
		System.exit(0);
	}
}
