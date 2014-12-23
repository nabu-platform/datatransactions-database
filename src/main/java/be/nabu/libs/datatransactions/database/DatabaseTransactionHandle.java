package be.nabu.libs.datatransactions.database;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.Date;

import be.nabu.libs.datatransactions.api.DataTransaction;
import be.nabu.libs.datatransactions.api.DataTransactionHandle;
import be.nabu.libs.datatransactions.api.DataTransactionState;
import be.nabu.libs.datatransactions.api.Transactionality;

public class DatabaseTransactionHandle implements DataTransactionHandle {

	private DatabaseTransaction<?> transaction;
	private DatabaseTransactionDAO dao;
	
	<T> DatabaseTransactionHandle(DatabaseTransactionDAO dao, DatabaseTransaction<?> transaction) {
		this.dao = dao;
		this.transaction = transaction;
	}

	@Override
	public void commit(URI response) throws IOException {
		if (DataTransactionState.STARTED.equals(transaction.getState())) {
			transaction.setResponse(response);
			if (Transactionality.ONE_PHASE.equals(transaction.getTransactionality())) {
				transaction.setDone(new Date());
				transaction.setState(DataTransactionState.DONE);
				try {
					dao.create(transaction);
				}
				catch (SQLException e) {
					throw new IOException(e);
				}
			}
			else {
				if (Transactionality.THREE_PHASE.equals(transaction.getTransactionality())) {
					transaction.setCommitted(new Date());
					transaction.setState(DataTransactionState.COMMITTED);
				}
				else {
					transaction.setDone(new Date());
					transaction.setState(DataTransactionState.DONE);
				}
				try {
					dao.update(transaction);
				}
				catch (SQLException e) {
					throw new IOException(e);
				}
			}
		}
	}

	@Override
	public void fail(String message) throws IOException {
		transaction.setMessage(message);
		transaction.setDone(new Date());
		transaction.setState(DataTransactionState.FAILED);
		try {
			dao.update(transaction);
		}
		catch (SQLException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void done() throws IOException {
		if (!DataTransactionState.DONE.equals(transaction.getState())) {
			transaction.setDone(new Date());
			transaction.setState(DataTransactionState.DONE);
			try {
				dao.update(transaction);
			}
			catch (SQLException e) {
				throw new IOException(e);
			}
		}
	}

	@Override
	public DataTransaction<?> getTransaction() {
		return transaction;
	}
}
