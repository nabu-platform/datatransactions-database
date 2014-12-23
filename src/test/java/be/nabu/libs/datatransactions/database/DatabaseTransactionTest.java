package be.nabu.libs.datatransactions.database;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

import javax.sql.DataSource;

import be.nabu.libs.datatransactions.api.DataTransaction;
import be.nabu.libs.datatransactions.api.DataTransactionBatch;
import be.nabu.libs.datatransactions.api.DataTransactionHandle;
import be.nabu.libs.datatransactions.api.DataTransactionState;
import be.nabu.libs.datatransactions.api.Direction;
import be.nabu.libs.datatransactions.api.ProviderResolver;
import be.nabu.libs.datatransactions.api.Transactionality;

public class DatabaseTransactionTest {
	
	public void testThreePhase(DataSource datasource) throws IOException {
		Date timestamp = new Date();
		SupposedProperties properties = new SupposedProperties();
		properties.setSomeValue("testing");
		DatabaseTransactionProvider provider = new DatabaseTransactionProvider(datasource, TimeZone.getDefault());
		DataTransactionBatch<String> batch = provider.newBatch(new EmptyProviderResolver<String>(), "test.threePhase", "me", null, Direction.IN, Transactionality.THREE_PHASE);
	
		DataTransactionHandle handle = batch.start("testProvider", properties, null);
		assertEquals(DataTransactionState.STARTED, provider.getTransaction(handle.getTransaction().getId()).getState());
		// make sure there is a "pending" transaction
		assertEquals(1, filter(provider.getPendingTransactions("me", timestamp), Transactionality.THREE_PHASE).size());
		
		handle.commit(null);
		assertEquals(DataTransactionState.COMMITTED, provider.getTransaction(handle.getTransaction().getId()).getState());
		// make sure there is still a "pending" transaction
		assertEquals(1, filter(provider.getPendingTransactions("me", timestamp), Transactionality.THREE_PHASE).size());
		
		handle.done();
		assertEquals(DataTransactionState.DONE, provider.getTransaction(handle.getTransaction().getId()).getState());
		// make sure it is no longer marked as "pending"
		assertEquals(0, filter(provider.getPendingTransactions("me", timestamp), Transactionality.THREE_PHASE).size());
	}
	
	public void testTwoPhase(DataSource datasource) throws IOException {
		// yesterday
		Date timestamp = new Date(new Date().getTime() - 60l*60l*1000l*24l);
		
		SupposedProperties properties = new SupposedProperties();
		properties.setSomeValue("testing");
		DatabaseTransactionProvider provider = new DatabaseTransactionProvider(datasource, TimeZone.getDefault());
		DataTransactionBatch<String> batch = provider.newBatch(new EmptyProviderResolver<String>(), "test.twoPhase", "me", null, Direction.IN, Transactionality.TWO_PHASE);

		DataTransactionHandle handle = batch.start("testProvider", properties, null);
		assertEquals(DataTransactionState.STARTED, provider.getTransaction(handle.getTransaction().getId()).getState());
		assertEquals(1, filter(provider.getPendingTransactions("me", timestamp), Transactionality.TWO_PHASE).size());
		// random check to see that the creator id is indeed taken into account
		assertEquals(0, filter(provider.getPendingTransactions("notme", timestamp), Transactionality.TWO_PHASE).size());
		
		handle.commit(null);
		assertEquals(DataTransactionState.DONE, provider.getTransaction(handle.getTransaction().getId()).getState());
		assertEquals(1, filter(provider.getPendingTransactions("me", timestamp), Transactionality.TWO_PHASE).size());
		
		handle.done();
		assertEquals(DataTransactionState.DONE, provider.getTransaction(handle.getTransaction().getId()).getState());
		// while it is still marked pending if we give it this timestamp
		assertEquals(1, filter(provider.getPendingTransactions("me", timestamp), Transactionality.TWO_PHASE).size());
		// it should not be pending if we take it after that
		assertEquals(0, filter(provider.getPendingTransactions("me", new Date(timestamp.getTime() + 2l*60l*60l*1000l*24l)), Transactionality.TWO_PHASE).size());
	}
	
	public void testOnePhase(DataSource datasource) throws IOException {
		// yesterday
		Date timestamp = new Date(new Date().getTime() - 60l*60l*1000l*24l);
		
		SupposedProperties properties = new SupposedProperties();
		properties.setSomeValue("testing");
		DatabaseTransactionProvider provider = new DatabaseTransactionProvider(datasource, TimeZone.getDefault());
		DataTransactionBatch<String> batch = provider.newBatch(new EmptyProviderResolver<String>(), "test.onePhase", "me", null, Direction.IN, Transactionality.ONE_PHASE);
		
		DataTransactionHandle handle = batch.start("testProvider", properties, null);
		// should not exist yet
		assertEquals(null, provider.getTransaction(handle.getTransaction().getId()));
		assertEquals(0, filter(provider.getPendingTransactions("me", timestamp), Transactionality.ONE_PHASE).size());
		
		handle.commit(null);
		assertEquals(DataTransactionState.DONE, provider.getTransaction(handle.getTransaction().getId()).getState());
		assertEquals(1, filter(provider.getPendingTransactions("me", timestamp), Transactionality.ONE_PHASE).size());
		
		handle.done();
		assertEquals(DataTransactionState.DONE, provider.getTransaction(handle.getTransaction().getId()).getState());
		// while it is still marked pending if we give it this timestamp
		assertEquals(1, filter(provider.getPendingTransactions("me", timestamp), Transactionality.ONE_PHASE).size());
		// it should not be pending if we take it after that
		assertEquals(0, filter(provider.getPendingTransactions("me", new Date(timestamp.getTime() + 2l*60l*60l*1000l*24l)), Transactionality.ONE_PHASE).size());
	}
	
	public void testBatch(DataSource datasource) throws IOException {
		SupposedProperties properties = new SupposedProperties();
		properties.setSomeValue("testing");
		DatabaseTransactionProvider provider = new DatabaseTransactionProvider(datasource, TimeZone.getDefault());
		DataTransactionBatch<String> batch = provider.newBatch(new EmptyProviderResolver<String>(), "test.batch", "me", null, Direction.IN, Transactionality.TWO_PHASE);
		
		DataTransactionHandle handle1 = batch.start("testProvider", properties, null);
		DataTransactionHandle handle2 = batch.start("testProvider", properties, null);
		
		List<DataTransaction<?>> transactions = provider.getBatch(handle1.getTransaction().getBatchId());
		assertEquals(2, transactions.size());
		assertEquals(DataTransactionState.STARTED, transactions.get(0).getState());
		assertEquals(DataTransactionState.STARTED, transactions.get(1).getState());
		
		handle1.commit(null);
		handle2.commit(null);
		
		transactions = provider.getBatch(handle1.getTransaction().getBatchId());
		assertEquals(2, transactions.size());
		assertEquals(DataTransactionState.DONE, transactions.get(0).getState());
		assertEquals(DataTransactionState.DONE, transactions.get(1).getState());
	}
	
	public static List<DataTransaction<?>> filter(List<DataTransaction<?>> items, Transactionality transactionality) {
		Iterator<DataTransaction<?>> iterator = items.iterator();
		while (iterator.hasNext()) {
			if (!transactionality.equals(iterator.next().getTransactionality())) {
				iterator.remove();
			}
		}
		return items;
	}
	
	public static void assertEquals(Object expected, Object actual) {
		if ((expected == null && actual != null) || (expected != null && !expected.equals(actual))) {
			throw new AssertionError("Expected " + expected + " does not match actual " + actual);
		}
	}
	
	public static class SupposedProperties {
		private String someValue;

		public String getSomeValue() {
			return someValue;
		}

		public void setSomeValue(String someValue) {
			this.someValue = someValue;
		}
	}
	
	public static class EmptyProviderResolver<T> implements ProviderResolver<T> {
		@Override
		public String getId(T provider) {
			return "testProviderId";
		}

		@Override
		public T getProvider(String id) {
			return null;
		}
	}
}
