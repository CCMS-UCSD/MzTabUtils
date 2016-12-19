package edu.ucsd.mztab.util;

import java.io.File;
import java.io.FileInputStream;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class DatabaseUtils
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String DB_CONFIG_FILE = "massive.properties";
	private static final String DB_DRIVER = "com.mysql.jdbc.Driver";
	private static final String DB_PROTOCOL = "jdbc:mysql";
	
	/*========================================================================
	 * Static properties
	 *========================================================================*/
	private static Properties dbConfig = null;
	private static String dbURL = null;
	static {
		dbConfig = loadDatabaseConfiguration();
		if (dbConfig != null) try {
			Class.forName(DB_DRIVER);
			dbURL = DB_PROTOCOL + "://";
			dbURL += dbConfig.getProperty("db.host");
			dbURL += "/" + dbConfig.getProperty("db.database");
			dbURL += "?user=" +
				URLEncoder.encode(dbConfig.getProperty("db.user"), "UTF-8");
			dbURL += "&password=" +
				URLEncoder.encode(dbConfig.getProperty("db.password"), "UTF-8");
		} catch (Throwable error) {
			System.err.println(
				"There was an error generating the database URL.");
			error.printStackTrace();
			dbURL = null;
		}
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static Connection getConnection() {
		if (dbURL == null)
			return null;
		else try {
			return DriverManager.getConnection(dbURL);
		} catch (Throwable error) {
			System.err.println(
				"There was an error obtaining a database connection.");
			error.printStackTrace();
			return null;
		}
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private static Properties loadDatabaseConfiguration() {
		Properties properties = new Properties();
		try {
			File appRoot = new File(DatabaseUtils.class.getProtectionDomain()
				.getCodeSource().getLocation().toURI()).getParentFile();
			properties.load(
				new FileInputStream(new File(appRoot, DB_CONFIG_FILE)));
		} catch (Throwable error) {
			System.err.println(
				"There was an error loading the database configuration.");
			error.printStackTrace();
			return null;
		}
		if (properties.isEmpty())
			return null;
		else return properties;
	}
}
