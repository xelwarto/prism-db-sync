/************************************
 * ConfigHandler.java
 *
 * Used to validate the configuration file has the correct/required
 * parameters defined
 *
 * Written by: Ted Elwartowski
 ************************************/
package org.jdog.prism.handler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.jdog.xml.xmlConfig;

/**
 * 
 * @author Ted Elwartowski
 */
public class ConfigHandler {

	private static String[] configNames = {
			// Source database config params
			"databases.source.name", "databases.source.type",
			"databases.source.driver", "databases.source.url",
			"databases.source.user",
			"databases.source.transactions",
			// Destination database config params
			"databases.destination.name", "databases.destination.type",
			"databases.destination.driver", "databases.destination.url",
			"databases.destination.user", "databases.destination.transactions" };

	public static void validate(xmlConfig config) throws Exception {
		if (config != null) {
			for (int c = 0; c < configNames.length; c++) {
				if (config.getProperty(configNames[c]) == null) {
					throw new Exception(
							"Config Error: the following configuration item is missing - "
									+ configNames[c]);
				}
			}
		} else {
			throw new Exception("Config Error: configuration object is null");
		}
	}

	public static List<HashMap<String, Object>> getTables(xmlConfig config,
			String prop, List<HashMap<String, Object>> tableList) {
		if (prop != null) {
			if (config.getString(prop + "[@name]") != null) {
				String propName = config.getString(prop + "[@name]");

				if (propName != null && !propName.equals("")) {
					List<HashMap<String, Object>> tables = new ArrayList<HashMap<String, Object>>();

					Iterator<HashMap<String, Object>> tableIt = tableList
							.iterator();
					while (tableIt.hasNext()) {
						HashMap<String, Object> table = (HashMap<String, Object>) tableIt
								.next();
						if (table != null) {
							HashMap<String, Object> newTable = new HashMap<String, Object>();
							newTable.put("FROM", (String) table.get("FROM"));
							newTable.put("TO", (String) table.get("TO"));
							newTable.put("SYNC", (Boolean) table.get("SYNC"));
							newTable.put("SYNCDATA",
									(Boolean) table.get("SYNCDATA"));

							if (propName.equalsIgnoreCase((String) newTable
									.get("FROM"))) {
								if (config.getProperty(prop + "[@sync]") != null) {
									if (config.getString(prop + "[@sync]")
											.equalsIgnoreCase("yes")) {
										newTable.put("SYNC", new Boolean(true));
									}
									if (config.getString(prop + "[@sync]")
											.equalsIgnoreCase("no")) {
										newTable.put("SYNC", new Boolean(false));
									}
								}
								if (config.getProperty(prop + "[@sync_data]") != null) {
									if (config.getString(prop + "[@sync_data]")
											.equalsIgnoreCase("yes")) {
										newTable.put("SYNCDATA", new Boolean(
												true));
									}
									if (config.getString(prop + "[@sync_data]")
											.equalsIgnoreCase("no")) {
										newTable.put("SYNCDATA", new Boolean(
												false));
									}
								}
								if (config.getProperty(prop + "[@to_table]") != null) {
									newTable.put(
											"TO",
											config.getProperty(prop
													+ "[@to_table]"));
								}
								tables.add(newTable);
							} else {
								tables.add(newTable);
							}
						}
					}
					return tables;
				}
			}
		}
		return tableList;
	}

	public static List<HashMap<String, Object>> getQueries(xmlConfig config) {
		List<HashMap<String, Object>> queries = new ArrayList<HashMap<String, Object>>();
		Object qryList = config.getProperty("queries.query[@name]");
		if (qryList != null) {
			if (qryList instanceof Collection) {
				for (int q = 0; q < ((Collection<?>) qryList).size(); q++) {
					String prop = "queries.query(" + q + ")";
					queries.add(ConfigHandler._getQuery(config, prop));
				}
			} else {
				String prop = "queries.query";
				queries.add(ConfigHandler._getQuery(config, prop));
			}
		}
		return queries;
	}

	private static HashMap<String, Object> _getQuery(xmlConfig config,
			String prop) {
		if (prop != null) {
			HashMap<String, Object> query = new HashMap<String, Object>();
			if (config.getString(prop + "[@name]") != null) {
				String queryName = config.getString(prop + "[@name]");
				if (queryName != null) {
					query.put("NAME", queryName);
				}
			}

			if (config.getString(prop + ".source") != null) {
				String sourceQuery = config.getString(prop + ".source");
				if (sourceQuery != null) {
					query.put("SRC", sourceQuery);
				}
			}

			if (config.getString(prop + ".destination") != null) {
				String destQuery = config.getString(prop + ".destination");
				if (destQuery != null) {
					query.put("DST", destQuery);
				}
			}

			if (config.getString(prop + ".checksum") != null) {
				String checkSum = config.getString(prop + ".checksum");
				if (checkSum != null) {
					query.put("CKSM", checkSum);
				}

				query.put("CKSMLVL", "Low");
				if (config.getString(prop + ".checksum[@level]") != null) {
					String checkSumLevel = config.getString(prop
							+ ".checksum[@level]");
					if (checkSumLevel != null) {
						query.put("CKSMLVL", checkSumLevel);
					}
				}
			}

			/*
			 * FIX HERE
			 * 
			 * The roll back section is currently not type match compliant
			 * 
			Object rlbkList = config.getProperty(prop + ".rollback.query");
			if (rlbkList != null) {
				List<HashMap<String, String>> rollBacks = new ArrayList<HashMap<String, String>>();
				if (rlbkList instanceof Collection) {
					for (int q = 0; q < ((Collection<?>) rlbkList).size(); q++) {
						String rlbkQuery = config.getString(prop
								+ ".rollback.query(" + q + ")");
						if (rlbkQuery != null) {
							String rlbkDataBase = config.getString(prop
									+ ".rollback.query(" + q + ")[@database]");
							if (rlbkDataBase != null
									&& (rlbkDataBase.equalsIgnoreCase("src") || rlbkDataBase
											.equalsIgnoreCase("dest"))) {
								HashMap<String, String> rollBack = new HashMap<String, String>();
								rollBack.put("QUERY", rlbkQuery);
								rollBack.put("DB", rlbkDataBase);
								rollBacks.add(rollBack);
							}
						}
					}
				} else {
					String rlbkQuery = config.getString(prop
							+ ".rollback.query");
					if (rlbkQuery != null) {
						String rlbkDataBase = config.getString(prop
								+ ".rollback.query[@database]");
						if (rlbkDataBase != null
								&& (rlbkDataBase.equalsIgnoreCase("src") || rlbkDataBase
										.equalsIgnoreCase("dest"))) {
							HashMap<String, String> rollBack = new HashMap<String, String>();
							rollBack.put("QUERY", rlbkQuery);
							rollBack.put("DB", rlbkDataBase);
							rollBacks.add(rollBack);
						}
					}
				}
				query.put("RLBK", rollBacks);
			}
			*/
			
			
			return query;
		}
		return null;
	}
}
