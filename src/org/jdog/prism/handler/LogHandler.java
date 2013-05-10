/************************************
 * LogHandler.java
 *
 * Used to handle the logging and error reporting for the application
 *
 * Written by: Ted Elwartowski
 ************************************/
package org.jdog.prism.handler;

import java.io.File;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;

import java.util.Date;
import java.text.SimpleDateFormat;

import org.jdog.prism.util.Variables;

/**
 * 
 * @author Ted Elwartowski
 */
public class LogHandler {

	private boolean quiet = false;
	private boolean debug = false;
	private boolean verbose = false;
	private PrintWriter out = null;

	public LogHandler() {
	}

	public boolean isDebug() {
		return debug;
	}

	public boolean isVerbose() {
		return verbose;
	}

	public boolean isQuiet() {
		return quiet;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
		this.debug = verbose;
	}

	public void setQuiet(boolean quiet) {
		this.quiet = quiet;
	}

	public void setLogFile(String logFile) throws Exception {
		if (logFile != null) {
			File file = new File(logFile);
			if (file.exists() && !file.canWrite()) {
				throw new Exception("Can not open log file for writing.");
			}
			out = new PrintWriter(new BufferedWriter(new FileWriter(logFile,
					true)));
		}
	}

	public void log(String message) {
		if (message != null) {
			if (!quiet) {
				System.out.print(getDate());
				System.out.println(message);
			}

			if (out != null) {
				out.print(getDate());
				out.println(message);
				out.flush();
			}
		}
	}

	public void debug(String message) {
		if (message != null) {
			if (debug) {
				if (!quiet) {
					System.out.print(getDate());
					if (!this.isVerbose()) {
						System.out.print(Variables.LOG_DEBUG);
					}
					System.out.println(message);
				}

				if (out != null) {
					out.print(getDate());
					if (!this.isVerbose()) {
						System.out.print(Variables.LOG_DEBUG);
					}
					out.println(message);
					out.flush();
				}
			}
		}
	}

	public void verbose(String message) {
		if (message != null) {
			if (verbose) {
				log(message);
			}
		}
	}

	public void close() throws Exception {
		if (out != null) {
			out.flush();
			out.close();
		}
	}

	private String getDate() {
		String dateOut = "[";
		Date date = new Date();
		SimpleDateFormat dateFmt = new SimpleDateFormat(Variables.LOG_DATE);
		dateOut = dateOut.concat(dateFmt.format(date));
		dateOut = dateOut.concat("] ");
		return dateOut;
	}
}
