package tdg.sorbas.cache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import org.apache.jena.ext.com.google.common.collect.Sets;

import com.google.common.collect.Lists;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import tdg.link_discovery.connector.sparql.evaluator.arq.linker.factory.SPARQLFactory;
import tdg.link_discovery.middleware.objects.Tuple;

public class H2Cache {

	
	private static HikariDataSource datasource;
	private static Integer maxPoolSize = 500;
	
	public H2Cache() {		
		datasource = createInitHikari(null);
		initCache();
	}
	
	public H2Cache(String file) {		
		datasource = createInitHikari(file);
		initCache();
	}

	

	
	private void initCache() {
		Connection connection = null;
		PreparedStatement statement = null;
		try{
			// Create connection
			connection = datasource.getConnection();
			connection.setAutoCommit(false);
			// Prepare query to create table
			statement = connection.prepareStatement("CREATE TABLE IF NOT EXISTS links ( "+
					" id int PRIMARY KEY AUTO_INCREMENT,\n" + 
					" rule varchar(1000),\n" + 
					" iri_source varchar(1000),\n" + 
					" iri_target varchar(1000)) ");
			// Execute query
			statement.executeUpdate();
			// Create voting table
			statement = connection.prepareStatement("CREATE TABLE IF NOT EXISTS neighbors_linked ( "+
					" id int PRIMARY KEY AUTO_INCREMENT,\n"+
					" source_iri varchar(1000) ,\n" + 
					" target_iri varchar(1000) ,\n" + 
					" supporting_rule varchar(1000) ,\n" + 
					" neighbor_source_path varchar(1000) ,\n" + 
					" neighbor_target_path varchar(1000) ,\n" +
					" neighbor_source_size int ,\n" + 
					" neighbor_target_size int ,\n" + 
					" neighbor_source_iri varchar(1000) ,\n" + 
					" neighbor_target_iri varchar(1000) , "+
					" main_rule varchar(1000) ,\n" + 
					" )");
			// Execute query
			statement.executeUpdate();
			
			// Close connection and statement
			statement.close();
			connection.close();
			 
		} catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	
		
	public void addLink(String rule, String iriSource, String iriTarget)  {
		Connection connection = null;
		PreparedStatement statement = null;
		StringBuilder query = null;
		
		//Build the query
		 query = new StringBuilder();
		query.append("INSERT INTO links (rule, iri_source, iri_target) VALUES ('");
		query.append(fixH2Literals(rule)).append("', '");
		query.append(fixH2Literals(iriSource)).append("', '");
		query.append(fixH2Literals(iriTarget)).append("' ) ");
		
		try {
			// Create connection
			connection = datasource.getConnection();
			connection.setAutoCommit(true); // set commit to true to save data
			// Prepare query to insert new data
			statement = connection.prepareStatement(query.toString());
			// Execute query
			statement.executeUpdate();
			// Close connection and statement
			statement.close();
			connection.close();
		} catch(SQLException e) {
			e.printStackTrace();	
		}
	}
	
	private String fixH2Literals(String literalToInsert) {
		return literalToInsert.replace("'", "''");
	}
	
	public void addNeighbourLink(String mainRule, String supportingRule, String iriSource, String iriTarget, String neighbourSourceIri, String neighbourTargetIri, String sourcePath, String targetPath, Integer sourceNeighborSize, Integer targetNeighborSize)  {
		Connection connection = null;
		PreparedStatement statement = null;
		StringBuilder query = null;
		// build query
		query = new StringBuilder();
		query.append("INSERT INTO neighbors_linked (source_iri, target_iri, supporting_rule, neighbor_source_path, neighbor_target_path, neighbor_source_size, neighbor_target_size, neighbor_source_iri, neighbor_target_iri, main_rule) VALUES ('");
		query.append(fixH2Literals(iriSource)).append("', '").append(fixH2Literals(iriTarget)).append("', '");
		query.append(fixH2Literals(supportingRule)).append("', '").append(fixH2Literals(sourcePath)).append("', '");
		query.append(fixH2Literals(targetPath)).append("', ").append(sourceNeighborSize).append(", ");
		query.append(targetNeighborSize).append(", '").append(fixH2Literals(neighbourSourceIri)).append("', '");
		query.append(fixH2Literals(neighbourTargetIri)).append("', '").append(fixH2Literals(mainRule)).append("')");
		
		try {
			// Create connection
			connection = datasource.getConnection();
			connection.setAutoCommit(true); // set commit to true to save data
			// Prepare query to insert new data
			statement = connection.prepareStatement(query.toString());
			// Execute query
			statement.executeUpdate();
			// Close connection and statement
			statement.close();
			connection.close();
		} catch(SQLException e) {
			e.printStackTrace();
			
		}
	}
	
	
	
	
	public Set<Tuple<String,String>> getLinks() {
		Connection connection = null; 
		PreparedStatement statement = null;
		ResultSet queryResult = null;
		Set<Tuple<String,String>> links = null;
		Tuple<String,String> link = null;
		try {
			// Create connection
			connection = datasource.getConnection();
			connection.setAutoCommit(false);
			// Prepare query to retrieve stored links
			statement = connection.prepareStatement("SELECT DISTINCT iri_source, iri_target FROM links");
			statement.setFetchSize(10000);
			// Execute query
			queryResult =  statement.executeQuery();
			links = Sets.newHashSet();
			while (queryResult.next()){
				// Retrieve instances
				String iriSource = queryResult.getString("iri_source");
				String iriTarget = queryResult.getString("iri_target");
				// Add new link to the output set
				link = new Tuple<String,String>(iriSource, iriTarget);
				links.add(link);
			}
			
			// Close connection, statement, and query results
			queryResult.close();
			statement.close();
			connection.close();
			 
		} catch(SQLException e) {
			e.printStackTrace();
		}
		return links;
	}
	
	// ------------ Testing code
	
	
	// ------------
	
	public void clearCache(){ // TODO: uncomment
		/*Connection connection = null;
		PreparedStatement statement = null;
		try {
			// Create connection
			connection = datasource.getConnection();
			connection.setAutoCommit(false);
			// Drop table links
			statement = connection.prepareStatement("DROP TABLE IF EXISTS links");
			statement.executeUpdate();
			// Drop table neighbors
			statement = connection.prepareStatement("DROP TABLE IF EXISTS neighbors_linked");
			statement.executeUpdate();
			
			// Close connection and statement
			statement.close();
			connection.close();
	
			// Crate again the empty table
			initCache();
		} catch(SQLException e) {
			e.printStackTrace();
		}*/
	}
	

	
	
	
	private static HikariDataSource createInitHikari(String file) {
		if(file==null || file.isEmpty())
			file = "./h2cache";
		HikariConfig config = new HikariConfig();
		
		config.setDataSourceClassName("org.h2.jdbcx.JdbcDataSource");
		config.setConnectionTestQuery("VALUES 1");
		//config.addDataSourceProperty("URL", "jdbc:h2:file:/Users/andrea/Desktop/data;MULTI_THREADED=1;CACHE_SIZE=2048");
		config.addDataSourceProperty("URL", "jdbc:h2:file:"+file+";MULTI_THREADED=1;CACHE_SIZE=2048");

		// config.addDataSourceProperty("cachePrepStmts", "true");
		// config.addDataSourceProperty("prepStmtCacheSize", "250");
		// config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
		// config.addDataSourceProperty("maximumPoolSize","20");
		HikariDataSource ds = new HikariDataSource(config);
		ds.setMaximumPoolSize(maxPoolSize);
		//ds.setConnectionTimeout(100000);
		return ds;
	}


	
	/* ===  Static methods === */
	
	

	public static String cacheFile = null;
	
	public static Connection getConnection() {
		Connection connection = null;
		if(datasource==null)
			datasource = createInitHikari(cacheFile);
		try {
			connection = datasource.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return connection;
	}
	
	public static List<String> retrieveLinks(String tableName) {
		Connection connection = null; 
		PreparedStatement statement = null;
		ResultSet queryResult = null;
		List<String> links = null;

		try {
			// Create connection
			connection = getConnection();
			connection.setAutoCommit(false);
			// Prepare query to retrieve stored links
			statement = connection.prepareStatement("SELECT DISTINCT iri_source, iri_target FROM "+tableName);
			statement.setFetchSize(10000);
			// Execute query
			queryResult =  statement.executeQuery();
			links = Lists.newArrayList();
			while (queryResult.next()){
				// Retrieve instances
				String iriSource = queryResult.getString("iri_source");
				String iriTarget = queryResult.getString("iri_target");
				// format link
				StringBuilder link = new StringBuilder();
				link.append(SPARQLFactory.fixIRIS(iriSource)).append(" <http://www.w3.org/2002/07/owl#sameAs> ").append(SPARQLFactory.fixIRIS(iriTarget)).append(" .");
				// Add new link to the output set
				links.add(link.toString());
			}
			
			// Close connection, statement, and query results
			queryResult.close();
			statement.close();
			connection.close();
		} catch(SQLException e) {
			e.printStackTrace();
		}
		return links;
		
	}

	/*
	 * Create or drops a table with an input query
	 */
	public static void updateTableQuery(String viewQuery) {
		Connection connection = null;
		PreparedStatement statement = null;
		try {
			
			// Create connection
			connection = getConnection();
			connection.setAutoCommit(true);
			// create view
			statement = connection.prepareStatement(viewQuery);
			statement.executeUpdate();
			// Close connection and statement
			statement.close();
			connection.close();
			
		} catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	
}
