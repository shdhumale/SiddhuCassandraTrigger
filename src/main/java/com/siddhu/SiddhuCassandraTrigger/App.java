package com.siddhu.SiddhuCassandraTrigger;

import java.util.ArrayList;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.cassandra.db.Mutation;
import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.Collection;

/**
 * Hello world!
 *
 */
public class App implements ITrigger {

	private static Logger logger = Logger.getLogger(App.class);

	public App() {
		logger.info("Cassandra trigger successfully initialized");
	}

	@Override
	public Collection<Mutation> augment(Partition update) {
		logger.info("Partition update received");
		try {
			process(update);
			logger.info("Processing partition update finished");
		} catch (Exception e) {
			logger.error("Processing partition update failed", e);
		}
		return new ArrayList<>();
	}

	private void process(Partition update) {

		logger.info("update.metadata().ksName:"+update.metadata().ksName);
		logger.info("update.metadata().toString():"+update.metadata().toString());
		final Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1")
				.build();		
		final Session session = cluster.connect("siddhu");

		try
		{
			logger.info("*********Cluster Information *************");
			logger.info(" Cluster Name is: " + cluster.getClusterName() );
			logger.info(" Driver Version is: " + cluster.getDriverVersion() );
			logger.info(" Cluster Configuration is: " + cluster.getConfiguration() );
			logger.info(" Cluster Metadata is: " + cluster.getMetadata() );
			logger.info(" Cluster Metrics is: " + cluster.getMetrics() );		

			// Retrieve all User details from Users table
			logger.info("*********Retrive User Data Example *************");		 
			getUsersAllDetails(session);

			// Insert new User into users table
			logger.info("*********Insert User Data Example *************");		
			session.execute("INSERT INTO sid_emp1 (emp_id, emp_name, emp_houseno) VALUES (1, 'siddhunew', 6011)");
			getUsersAllDetails(session);

		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			// Close Cluster and Session objects
			logger.info("nIs Cluster Closed :" + cluster.isClosed());
			logger.info("Is Session Closed :" + session.isClosed());		
			cluster.close();
			session.close();
			logger.info("Is Cluster Closed :" + cluster.isClosed());
			logger.info("Is Session Closed :" + session.isClosed());
		}



	}
	private static void getUsersAllDetails(final Session inSession){		
		// Use select to get the users table data
		ResultSet results = inSession.execute("SELECT * FROM sid_emp");
		for (Row row : results) {
			logger.info("emp name:"+row.getString("emp_name"));
			logger.info("emp id:"+row.getInt("emp_id"));
			logger.info("emp houseno:"+row.getVarint("emp_houseno"));

		}
	}
}
