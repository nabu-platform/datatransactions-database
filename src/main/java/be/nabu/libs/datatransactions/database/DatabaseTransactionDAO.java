package be.nabu.libs.datatransactions.database;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import javax.sql.DataSource;

import be.nabu.libs.converter.ConverterFactory;
import be.nabu.libs.converter.api.Converter;
import be.nabu.libs.datatransactions.api.DataTransaction;
import be.nabu.libs.datatransactions.api.DataTransactionState;
import be.nabu.libs.datatransactions.api.Direction;
import be.nabu.libs.datatransactions.api.Transactionality;
import be.nabu.libs.resources.URIUtils;
import be.nabu.libs.types.DefinedTypeResolverFactory;
import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.DefinedType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.java.BeanInstance;

public class DatabaseTransactionDAO {
	
	private DataSource datasource;
	private TimeZone timezone;
	private Converter converter = ConverterFactory.getInstance().getConverter();

	public DatabaseTransactionDAO(DataSource datasource, TimeZone timezone) {
		this.datasource = datasource;
		this.timezone = timezone;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private DatabaseTransaction<?> build(Connection connection, ResultSet result) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException, URISyntaxException {
		String id = result.getString("id");
		String batchId = result.getString("batch_id");
		Date started = result.getTimestamp("started", Calendar.getInstance(timezone));
		Date committed = result.getTimestamp("committed", Calendar.getInstance(timezone));
		Date done = result.getTimestamp("done", Calendar.getInstance(timezone));
		String state = result.getString("state");
		String message = result.getString("message");
		String providerId = result.getString("provider_id");
		String request = result.getString("request");
		String response = result.getString("response");
		String direction = result.getString("direction");
		String context = result.getString("context");
		String sourceId = result.getString("source_id");
		String creatorId = result.getString("creator_id");
		String propertiesTypeId = result.getString("properties_type_id");
		String transactionality = result.getString("transactionality");
		
		ComplexType propertiesType = (ComplexType) DefinedTypeResolverFactory.getInstance().getResolver().resolve(propertiesTypeId);
		ComplexContent complexContent = propertiesType.newInstance();
		PreparedStatement statement = connection.prepareStatement("select name,value from data_transaction_properties where data_transaction_id = ?");
		statement.setString(1, id);
		ResultSet results = statement.executeQuery();
		while (results.next()) {
			complexContent.set(results.getString("name"), results.getString("value"));
		}
		// if it's a bean, cast it to the bean itself
		Object properties = complexContent instanceof BeanInstance ? TypeUtils.getAsBean(complexContent, ((BeanInstance) complexContent).getType().getBeanClass()) : complexContent;
		DatabaseTransaction transaction = new DatabaseTransaction();
		transaction.setContext(context);
		transaction.setCreatorId(creatorId);
		transaction.setSourceId(sourceId);
		transaction.setBatchId(batchId);
		transaction.setProviderId(providerId);
		transaction.setProperties(properties);
		if (request != null) {
			transaction.setRequest(new URI(URIUtils.encodeURI(request)));
		}
		transaction.setDirection(Enum.valueOf(Direction.class, direction));
		transaction.setProperties(propertiesType);
		transaction.setStarted(started);
		transaction.setCommitted(committed);
		transaction.setDone(done);
		transaction.setId(id);
		if (response != null) {
			transaction.setResponse(new URI(URIUtils.encodeURI(response)));
		}
		transaction.setState(Enum.valueOf(DataTransactionState.class, state));
		transaction.setMessage(message);
		transaction.setTransactionality(Enum.valueOf(Transactionality.class, transactionality));
		return transaction;
	}
	
	public List<DataTransaction<?>> getPending(String creatorId, Date from) throws SQLException {
		Connection connection = getConnection();
		try {
			String sql = "select id,batch_id,started,committed,done,state,message,provider_id,request,response,transactionality,direction,context,source_id,creator_id,properties_type_id from data_transactions where creator_id = ? and (";
			// ONE PHASE
			sql += " (transactionality = ? and state = ? and started >= ?)";
			// TWO PHASE - START
			sql += " or (transactionality = ? and state = ?)";
			// TWO PHASE - DONE
			sql += " or (transactionality = ? and state = ? and started >= ?)";
			// THREE PHASE
			sql += " or (transactionality = ? and (state = ? or state = ?))";
			sql += ")";
			
			PreparedStatement statement = connection.prepareStatement(sql);
			int counter = 1;
			statement.setString(counter++, creatorId);
			// ONE PHASE
			statement.setString(counter++, Transactionality.ONE_PHASE.name());
			statement.setString(counter++, DataTransactionState.DONE.name());
			statement.setTimestamp(counter++, new java.sql.Timestamp(from.getTime()), Calendar.getInstance(timezone));
			// TWO PHASE - START
			statement.setString(counter++, Transactionality.TWO_PHASE.name());
			statement.setString(counter++, DataTransactionState.STARTED.name());
			// TWO PHASE - DONE
			statement.setString(counter++, Transactionality.TWO_PHASE.name());
			statement.setString(counter++, DataTransactionState.DONE.name());
			statement.setTimestamp(counter++, new java.sql.Timestamp(from.getTime()), Calendar.getInstance(timezone));
			// THREE PHASE
			statement.setString(counter++, Transactionality.THREE_PHASE.name());
			statement.setString(counter++, DataTransactionState.STARTED.name());
			statement.setString(counter++, DataTransactionState.COMMITTED.name());
			
			ResultSet result = statement.executeQuery();
			List<DataTransaction<?>> transactions = new ArrayList<DataTransaction<?>>();
			while (result.next()) {
				transactions.add(build(connection, result));
			}
			commit(connection);
			return transactions;
		}
		catch(SQLException e) {
			rollback(connection);
			throw e;
		}
		catch (InstantiationException e) {
			rollback(connection);
			throw new RuntimeException(e);
		}
		catch (IllegalAccessException e) {
			rollback(connection);
			throw new RuntimeException(e);
		}
		catch (ClassNotFoundException e) {
			rollback(connection);
			throw new RuntimeException(e);
		}
		catch (URISyntaxException e) {
			rollback(connection);
			throw new RuntimeException(e);
		}
	}

	public List<DataTransaction<?>> getBatch(String batchId) throws SQLException {
		Connection connection = getConnection();
		try {
			PreparedStatement statement = connection.prepareStatement("select id,batch_id,started,committed,done,state,message,provider_id,request,response,transactionality,direction,context,source_id,creator_id,properties_type_id from data_transactions where batch_id = ?");
			statement.setString(1, batchId);
			ResultSet result = statement.executeQuery();
			List<DataTransaction<?>> transactions = new ArrayList<DataTransaction<?>>();
			while (result.next()) {
				transactions.add(build(connection, result));
			}
			commit(connection);
			return transactions;
		}
		catch(SQLException e) {
			rollback(connection);
			throw e;
		}
		catch (InstantiationException e) {
			rollback(connection);
			throw new RuntimeException(e);
		}
		catch (IllegalAccessException e) {
			rollback(connection);
			throw new RuntimeException(e);
		}
		catch (ClassNotFoundException e) {
			rollback(connection);
			throw new RuntimeException(e);
		}
		catch (URISyntaxException e) {
			rollback(connection);
			throw new RuntimeException(e);
		}
	}
	
	public DatabaseTransaction<?> getTransaction(String transactionId) throws SQLException {
		Connection connection = getConnection();
		try {
			PreparedStatement statement = connection.prepareStatement("select id,batch_id,started,committed,done,state,message,provider_id,request,response,transactionality,direction,context,source_id,creator_id,properties_type_id from data_transactions where id = ?");
			statement.setString(1, transactionId);
			ResultSet result = statement.executeQuery();
			DatabaseTransaction<?> transaction = result.next() ? build(connection, result) : null;
			commit(connection);
			return transaction;
		}
		catch(SQLException e) {
			rollback(connection);
			throw e;
		}
		catch (InstantiationException e) {
			rollback(connection);
			throw new RuntimeException(e);
		}
		catch (IllegalAccessException e) {
			rollback(connection);
			throw new RuntimeException(e);
		}
		catch (ClassNotFoundException e) {
			rollback(connection);
			throw new RuntimeException(e);
		}
		catch (URISyntaxException e) {
			rollback(connection);
			throw new RuntimeException(e);
		}
	}
	
	public void update(DataTransaction<?> transaction) throws SQLException {
		Connection connection = getConnection();
		try {
			merge(transaction, connection, true);
			commit(connection);
		}
		catch(SQLException e) {
			rollback(connection);
			throw e;
		}
	}
	
	@SuppressWarnings("rawtypes")
	public void create(DataTransaction<?> transaction) throws SQLException {
		Connection connection = getConnection();
		try {
			// first we create a record for the transaction
			merge(transaction, connection, false);
			
			ComplexContent properties = transaction.getProperties() instanceof ComplexContent 
				? (ComplexContent) transaction.getProperties()
				: new BeanInstance(transaction.getProperties());
					
			// next we create records for the properties			
			PreparedStatement statement = connection.prepareStatement("insert into data_transaction_properties (data_transaction_id, name, value) values (?,?,?)");
			boolean hasAny = false;
			for (Element<?> child : TypeUtils.getAllChildren(properties.getType())) {
				String value = converter.convert(properties.get(child.getName()), String.class);
				if (value != null) {
					hasAny = true;
					statement.setString(1, transaction.getId());
					statement.setString(2, child.getName());
					statement.setString(3, value);
					statement.addBatch();
				}
			}
			if (hasAny) {
				statement.executeBatch();
			}
			commit(connection);
		}
		catch (SQLException e) {
			rollback(connection);
			throw e;
		}
	}

	@SuppressWarnings({ "rawtypes" })
	private void merge(DataTransaction<?> transaction, Connection connection, boolean update) throws SQLException {
		String sql = update
			? "update data_transactions set batch_id=?,started=?,committed=?,done=?,state=?,message=?,provider_id=?,request=?,response=?,transactionality=?,direction=?,context=?,source_id=?,creator_id=?,properties_type_id=? where id = ?"
			: "insert into data_transactions (id,batch_id,started,committed,done,state,message,provider_id,request,response,transactionality,direction,context,source_id,creator_id,properties_type_id) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

		PreparedStatement statement = connection.prepareStatement(sql);
			
		ComplexContent properties = transaction.getProperties() instanceof ComplexContent 
			? (ComplexContent) transaction.getProperties()
			: new BeanInstance(transaction.getProperties());
		
		int counter = 1;
		if (!update) {
			statement.setString(counter++, transaction.getId());
		}
		statement.setString(counter++, transaction.getBatchId());
		statement.setTimestamp(counter++, new java.sql.Timestamp(transaction.getStarted().getTime()), Calendar.getInstance(timezone));
		statement.setTimestamp(counter++, transaction.getCommitted() != null ? new java.sql.Timestamp(transaction.getCommitted().getTime()) : null, Calendar.getInstance(timezone));
		statement.setTimestamp(counter++, transaction.getDone() != null ? new java.sql.Timestamp(transaction.getDone().getTime()) : null, Calendar.getInstance(timezone));
		statement.setString(counter++, transaction.getState().name());
		statement.setString(counter++, transaction.getMessage());
		statement.setString(counter++, transaction.getProviderId());
		statement.setString(counter++, transaction.getRequest() == null ? null : transaction.getRequest().toString());
		statement.setString(counter++, transaction.getResponse() == null ? null : transaction.getResponse().toString());
		statement.setString(counter++, transaction.getTransactionality().name());
		statement.setString(counter++, transaction.getDirection().name());
		statement.setString(counter++, transaction.getContext());
		statement.setString(counter++, transaction.getSourceId());
		statement.setString(counter++, transaction.getCreatorId());
		statement.setString(counter++, properties != null ? ((DefinedType) properties.getType()).getId() : null);
		if (update) {
			statement.setString(counter++, transaction.getId());
		}
		if (statement.executeUpdate() != 1) {
			throw new SQLException("Could not merge the data transaction");
		}
	}
	
	private void rollback(Connection connection) throws SQLException {
		try {
			if (!connection.getAutoCommit() && connection.getTransactionIsolation() != Connection.TRANSACTION_NONE) {
				connection.rollback();
			}
		}
		catch (SQLException e) {
			// ignore
		}
		connection.close();
	}

	private void commit(Connection connection) throws SQLException {
		if (!connection.getAutoCommit() && connection.getTransactionIsolation() != Connection.TRANSACTION_NONE) {
			connection.commit();
		}
		connection.close();
	}
	
	private Connection getConnection() throws SQLException {
		Connection connection = datasource.getConnection();
		connection.setAutoCommit(false);
		return connection;
	}

}
