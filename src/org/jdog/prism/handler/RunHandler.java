/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
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
public class RunHandler {

	private xmlConfig config = null;
	private LogHandler log = null;

	public RunHandler(LogHandler log, xmlConfig config) {
		this.log = log;
		this.config = config;
	}

	public void run() {
		DataBaseHandler srcDatabase = null;
		DataBaseHandler destDatabase = null;

		try {
			log.debug("Validating the configuration file");
			ConfigHandler.validate(config);
			srcDatabase = new DataBaseHandler(
					config.getString("databases.source.type"),
					config.getString("databases.source.name"));
			destDatabase = new DataBaseHandler(
					config.getString("databases.destination.type"),
					config.getString("databases.destination.name"));
		} catch (Exception e) {
			log.log(e.toString());
			srcDatabase = null;
			destDatabase = null;
		}

		if (srcDatabase != null && destDatabase != null) {
			try {
				String srcPassword = config
						.getString("databases.source.password");

				String destPassword = config
						.getString("databases.destination.password");

				boolean srcTrans = false;
				boolean destTrans = false;
				if (config.getString("databases.source.transactions") != null) {
					if (config.getString("databases.source.transactions")
							.equalsIgnoreCase("yes")) {
						srcTrans = true;
					}
				}
				if (config.getString("databases.destination.transactions") != null) {
					if (config.getString("databases.destination.transactions")
							.equalsIgnoreCase("yes")) {
						destTrans = true;
					}
				}

				log.log("RunHandler: Attempting to connect to source database ("
						+ srcDatabase.getType()
						+ ":"
						+ srcDatabase.getName()
						+ ")");
				if (srcTrans) {
					log.verbose("RunHandler: Setting source database to use transactions");
					srcDatabase.setTransactional(true);
				}
				srcDatabase
						.getConnection(new String[] {
								config.getString("databases.source.driver"),
								config.getString("databases.source.url"),
								config.getString("databases.source.user"),
								srcPassword });

				log.log("RunHandler: Attempting to connect to destination database ("
						+ destDatabase.getType()
						+ ":"
						+ destDatabase.getName()
						+ ")");
				if (destTrans) {
					log.verbose("RunHandler: Setting destination database to use transactions");
					destDatabase.setTransactional(true);
				}
				destDatabase.getConnection(new String[] {
						config.getString("databases.destination.driver"),
						config.getString("databases.destination.url"),
						config.getString("databases.destination.user"),
						destPassword });
			} catch (Exception e) {
				log.log("RunHandler Error: " + e.toString());
				srcDatabase = null;
				destDatabase = null;
			}
		}

		if (srcDatabase != null && destDatabase != null) {
			if (config.getString("tables[@sync_all]") != null) {
				List<String> fromTables = null;
				List<String> toTables = null;
				try {
					fromTables = srcDatabase.getTables();
					toTables = destDatabase.getTables();
				} catch (Exception e) {
					log.log("RunHandler Error: " + e.toString());
					fromTables = null;
					toTables = null;
				}

				SyncHandler sync = null;
				if (fromTables != null && toTables != null) {
					log.verbose("RunHandler: Number of source tables found: "
							+ fromTables.size());
					log.verbose("RunHandler: Number of destination tables found: "
							+ toTables.size());
					try {
						sync = new SyncHandler(log);
						sync.setDest(destDatabase);
						sync.setSrc(srcDatabase);
						sync.setFromTables(fromTables);
						sync.setToTables(toTables);
					} catch (Exception e) {
						log.log("RunHandler Error: " + e.toString());
						sync = null;
					}
				}

				if (sync != null) {
					List<HashMap<String, Object>> syncTables = new ArrayList<HashMap<String, Object>>();

					Boolean defSyncData = new Boolean(false);
					if (config.getString("tables[@sync_data]") != null) {
						if (config.getString("tables[@sync_data]")
								.equalsIgnoreCase("yes")) {
							defSyncData = new Boolean(true);
						}
					}

					Boolean syncAll = new Boolean(true);
					if (config.getString("tables[@sync_all]") != null) {
						if (config.getString("tables[@sync_all]")
								.equalsIgnoreCase("no")) {
							syncAll = new Boolean(false);
						}
					}

					Iterator<String> it = fromTables.iterator();
					while (it.hasNext()) {
						String tableName = (String) it.next();
						if (tableName != null) {
							HashMap<String, Object> table = new HashMap<String, Object>();
							table.put("FROM", tableName);
							table.put("TO", tableName);
							table.put("SYNC", syncAll);
							table.put("SYNCDATA", defSyncData);
							table.put("BEFORE", null);
							table.put("AFTER", null);
							syncTables.add(table);
						}
					}

					if (config.getProperty("tables.table[@name]") != null) {
						Object tblList = config
								.getProperty("tables.table[@name]");
						if (tblList instanceof Collection) {
							for (int t = 0; t < ((Collection<?>) tblList)
									.size(); t++) {
								String prop = "tables.table(" + t + ")";
								syncTables = ConfigHandler.getTables(config,
										prop, syncTables);
							}
						} else {
							String prop = "tables.table";
							syncTables = ConfigHandler.getTables(config, prop,
									syncTables);

						}
					}

					log.debug("RunHandler: Ordering sync table list");
					syncTables = ConfigHandler.orderTables(syncTables);

					if (syncTables != null) {
						if (syncTables.size() > 0) {
							Iterator<HashMap<String, Object>> tableIt = syncTables
									.iterator();
							while (tableIt.hasNext()) {
								HashMap<String, Object> table = (HashMap<String, Object>) tableIt
										.next();
								if (table != null) {
									sync.syncTable(table);
									if (sync.hasError()) {
										if (config
												.getString("app.errors[@stop]") != null) {
											if (config.getString(
													"app.errors[@stop]")
													.equalsIgnoreCase("yes")) {
												break;
											}
										}
									}
								}
							}
						} else {
							log.debug("RunHandler: syncTables is empty");
						}
					} else {
						log.log("RunHandler Error: null pointer exception (syncTables)");
					}
				}
			}

			// ## Run Queries Here ##
			List<HashMap<String, Object>> queries = ConfigHandler
					.getQueries(config);
			if (queries != null && queries.size() > 0) {
				QueryHandler qry = null;
				try {
					qry = new QueryHandler(log);
					qry.setSrc(srcDatabase);
					qry.setDest(destDatabase);
				} catch (Exception e) {
					log.log("RunHandler Error: " + e.toString());
					qry = null;
				}
				if (qry != null) {
					try {
						if (srcDatabase.isTransactional()
								|| destDatabase.isTransactional()) {

							/*
							 * FIX HERE
							 * 
							 * 
							 * Iterator<HashMap<String, Object>> it = queries
							 * .iterator(); while (it.hasNext()) {
							 * HashMap<String, Object> query = (HashMap<String,
							 * Object>) it .next(); if (query.get("RLBK") !=
							 * null) { List<?> rollBacks = (List<?>) query
							 * .get("RLBK"); if (rollBacks != null &&
							 * rollBacks.size() > 0) { throw new Exception(
							 * "Rollback queries not allowed when database transactions are enabled"
							 * ); } }
							 * 
							 * }
							 */

						}

						Iterator<HashMap<String, Object>> it = queries
								.iterator();
						while (it.hasNext()) {
							HashMap<String, Object> query = (HashMap<String, Object>) it
									.next();
							qry.syncQueries(query);

							if (qry.hasError()) {
								if (config.getString("app.errors[@stop]") != null) {
									if (config.getString("app.errors[@stop]")
											.equalsIgnoreCase("yes")) {
										break;
									}
								}
							}
						}
						if (!qry.hasError()) {
							qry.commit();
						}
						/*
						 * FIX HERE if (qry.hasError()) { qry.rollBack(); }
						 */
					} catch (Exception e) {
						log.log("RunHandler Error: " + e.toString());
					}
				}
			}

			// ## Close database connections ##
			try {
				if (srcDatabase != null) {
					log.debug("RunHandler: Attempting to close connection to source database");
					srcDatabase.close();
				}
			} catch (Exception e) {
				log.log("RunHandler Error: " + e.toString());
			}

			try {
				if (destDatabase != null) {
					log.debug("RunHandler: Attempting to close connection to destination database");
					destDatabase.close();
				}
			} catch (Exception e) {
				log.log("RunHandler Error: " + e.toString());
			}
		}
	}
}
