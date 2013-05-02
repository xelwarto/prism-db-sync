/************************************
 * SyncHandler.java
 *
 * Used to sync the data between the tables
 *
 * Written by: Ted Elwartowski
 ************************************/
package org.jdog.prism.handler;

import java.util.HashMap;
import java.util.List;

/**
 * 
 * @author Ted Elwartowski
 */
public class SyncHandler {

	private DataBaseHandler srcDatabase = null;
	private DataBaseHandler destDatabase = null;
	private List<String> fromTables = null;
	private List<String> toTables = null;
	private LogHandler log = null;
	private boolean hasError = false;

	public SyncHandler(LogHandler log) throws Exception {
		if (log != null) {
			this.log = log;
		} else {
			throw new Exception(
					"SyncHandler Error: null pointer exception (log)");
		}
	}

	public void setDest(DataBaseHandler destDatabase) throws Exception {
		if (destDatabase != null) {
			this.destDatabase = destDatabase;
		} else {
			throw new Exception(
					"SyncHandler Error: null pointer exception (destDatabase)");
		}
	}

	public void setSrc(DataBaseHandler srcDatabase) throws Exception {
		if (srcDatabase != null) {
			this.srcDatabase = srcDatabase;
		} else {
			throw new Exception(
					"SyncHandler Error: null pointer exception (srcDatabase)");
		}
	}

	public boolean hasError() {
		return hasError;
	}

	public void setFromTables(List<String> fromTables) {
		this.fromTables = fromTables;
	}

	public void setToTables(List<String> toTables) {
		this.toTables = toTables;
	}

	public void syncTable(HashMap<String, Object> syncConf) {
		hasError = false;
		if (destDatabase != null && srcDatabase != null) {
			_syncTable(syncConf);
		} else {
			_log("SyncHandler Error: null pointer exception (DataBaseHanlder)");
		}

	}

	private void _syncTable(HashMap<String, Object> syncConf) {
		List<Object[]> dataStore = null;
		if (syncConf != null) {
			String fromTable = (String) syncConf.get("FROM");
			String toTable = (String) syncConf.get("TO");
			Boolean sync = (Boolean) syncConf.get("SYNC");
			Boolean syncData = (Boolean) syncConf.get("SYNCDATA");
			if (sync == null) {
				sync = new Boolean(false);
			}
			if (syncData == null) {
				syncData = new Boolean(false);
			}

			if (fromTable != null && toTable != null) {
				if (fromTables != null && toTables != null) {
					if (sync != null && sync.booleanValue()) {
						if (fromTables.contains(fromTable)) {
							if (toTables.contains(toTable)) {

								log.log("SyncHandler: Attempting to sync source table("
										+ fromTable
										+ ") "
										+ "to destination table("
										+ toTable
										+ ")");
								List<String> fromFields = null;
								List<String> toFields = null;

								try {
									fromFields = srcDatabase
											.getColumnList(fromTable);
									log.verbose("SyncHandler: Number of source columns: "
											+ fromFields.size());
									toFields = destDatabase
											.getColumnList(toTable);
									log.verbose("SyncHandler: Number of destination columns: "
											+ toFields.size());
								} catch (Exception e) {
									_log("SyncHandler Error: " + e.toString());
									fromFields = null;
									toFields = null;
								}

								if (fromFields != null && toFields != null) {
									if (_compareColumns(fromFields, toFields)) {
										if (syncData != null
												&& syncData.booleanValue()) {
											int srcRecordCnt = 0;
											dataStore = null;
											try {
												log.verbose("SyncHandler: Getting source data set");
												dataStore = srcDatabase
														.unloadTable(fromTable);
												log.verbose("SyncHandler: Getting source record count");
												srcRecordCnt = srcDatabase
														.getRecordCount(fromTable);
												log.verbose("SyncHandler: Source records count: "
														+ srcRecordCnt);
											} catch (Exception e) {
												_log("SyncHandler Error: "
														+ e.toString());
												dataStore = null;
											}

											if (dataStore != null) {
												if (!dataStore.isEmpty()) {
													if (dataStore.size() == srcRecordCnt
															&& srcRecordCnt != 0) {
														log.verbose("SyncHandler: Number of source data set records found: "
																+ dataStore
																		.size());
														try {
															log.verbose("SyncHandler: Clearing data from destination table");
															destDatabase
																	.clearTable(toTable);
															log.verbose("SyncHandler: Loading data into destination table");
															destDatabase
																	.loadTable(
																			toTable,
																			dataStore);

															if (destDatabase
																	.isTransactional()) {
																log.verbose("SyncHandler: Commiting destination database changes");
																destDatabase
																		.commit();
															}

															log.verbose("SyncHandler: Getting destination record count");
															int destRecordCnt = 0;
															destRecordCnt = destDatabase
																	.getRecordCount(toTable);
															log.debug("SyncHandler: Validating destination record count with source");
															if (srcRecordCnt != destRecordCnt) {
																_log("SyncHandler Error: Source record count does not match destination record count");
															}
														} catch (Exception e) {
															_log("SyncHandler Error: "
																	+ e.toString());
															if (destDatabase
																	.isTransactional()) {
																try {
																	log.log("SyncHandler: Attempting to roll back destination database changes");
																	destDatabase
																			.rollBack();
																} catch (Exception e2) {
																	_log("SyncHandler Error: "
																			+ e2.toString());
																}
															}
														}
													} else {
														_log("SyncHandler Error: Data set in source table does not match record count");
													}
												} else {
													log.debug("SyncHandler Warn: Empty data set found in source table");
												}
											} else {
												_log("SyncHandler Error: null pointer exception (dataStore)");
											}
										} else {
											log.debug("SyncHandler: Skipping data sync for table");
										}
									} else {
										_debug("SyncHandler Error: Source columns do not match destination columns");
									}
								} else {
									_log("SyncHandler Error: null pointer exception (fromFields|toFields)");
								}
							} else {
								_log("SyncHandler Error: Destination table("
										+ toTable
										+ ") not found in reference tables");
							}
						} else {
							_log("SyncHandler Error: Source table(" + fromTable
									+ ") not found in reference tables");
						}
					} else {
						log.verbose("SyncHandler: Skipping sync for table("
								+ fromTable + ")");
					}
				} else {
					_log("SyncHandler Error: null pointer exception (fromTables|toTables)");
				}
			} else {
				_log("SyncHandler Error: null pointer exception (fromTable|toTable)");
			}
		} else {
			_log("SyncHandler Error: null pointer exception (syncConf)");
		}
		dataStore = null;
	}

	private boolean _compareColumns(List<String> fromFields,
			List<String> toFields) {
		boolean compare = true;
		if (fromFields != null && toFields != null) {
			for (int i = 0; i < fromFields.size(); i++) {
				if (!(i >= toFields.size())) {
					if (fromFields.get(i) != null && toFields.get(i) != null) {
						if (!fromFields.get(i)
								.equalsIgnoreCase(toFields.get(i))) {
							log.verbose("SyncHandler Error: source column("
									+ fromFields.get(i) + ") "
									+ "does not match destination column("
									+ toFields.get(i) + ") " + "at position: "
									+ Integer.toString(i));
							compare = false;
						}
					}
				} else {
					log.verbose("SyncHandler Error: source column("
							+ fromFields.get(i) + ") "
							+ "not in destination table");
				}
			}

			if (toFields.size() > fromFields.size()) {
				compare = false;
				for (int i = fromFields.size(); i < toFields.size(); i++) {
					log.verbose("SyncHandler Error: destination column("
							+ toFields.get(i) + ") " + "not in source table");
				}
			}
		} else {
			compare = false;
		}
		return compare;
	}

	private void _log(String msg) {
		hasError = true;
		if (log != null && msg != null) {
			log.log(msg);
		}
	}

	private void _debug(String msg) {
		hasError = true;
		if (log != null && msg != null) {
			log.debug(msg);
		}
	}
}
