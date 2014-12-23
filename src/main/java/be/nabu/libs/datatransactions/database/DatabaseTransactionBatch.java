package be.nabu.libs.datatransactions.database;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.Date;
import java.util.UUID;

import be.nabu.libs.datatransactions.DataTransactionUtils;
import be.nabu.libs.datatransactions.api.DataTransactionBatch;
import be.nabu.libs.datatransactions.api.DataTransactionHandle;
import be.nabu.libs.datatransactions.api.DataTransactionState;
import be.nabu.libs.datatransactions.api.Direction;
import be.nabu.libs.datatransactions.api.ProviderResolver;
import be.nabu.libs.datatransactions.api.Transactionality;

public class DatabaseTransactionBatch<T> implements DataTransactionBatch<T> {

	private String batchId = UUID.randomUUID().toString();
	private Direction direction;
	private String sourceId;
	private String creatorId;
	private DatabaseTransactionDAO dao;
	private String context;
	private Transactionality transactionality;
	private ProviderResolver<T> providerResolver;
	
	public DatabaseTransactionBatch(ProviderResolver<T> providerResolver, DatabaseTransactionDAO databaseTransactionDAO, String context, String creatorId, String sourceId, Direction direction, Transactionality transactionality) {
		this.providerResolver = providerResolver;
		this.dao = databaseTransactionDAO;
		this.context = context;
		this.creatorId = creatorId == null ? DataTransactionUtils.generateCreatorId() : creatorId;
		this.sourceId = sourceId;
		this.direction = direction;
		this.transactionality = transactionality == null ? Transactionality.THREE_PHASE : transactionality;
	}

	@Override
	public <P> DataTransactionHandle start(T provider, P properties, URI request) throws IOException {
		DatabaseTransaction<P> transaction = new DatabaseTransaction<P>();
		transaction.setId(UUID.randomUUID().toString());
		transaction.setState(DataTransactionState.STARTED);
		transaction.setStarted(new Date());
		transaction.setContext(context);
		transaction.setCreatorId(creatorId);
		transaction.setSourceId(sourceId);
		transaction.setBatchId(getId());
		transaction.setProviderId(providerResolver.getId(provider));
		transaction.setProperties(properties);
		transaction.setRequest(request);
		transaction.setDirection(direction);
		transaction.setTransactionality(transactionality);
		try {
			if (!Transactionality.ONE_PHASE.equals(transactionality)) {
				dao.create(transaction);
			}
			return new DatabaseTransactionHandle(dao, transaction);
		}
		catch (SQLException e) {
			throw new IOException(e);
		}
	}

	@Override
	public String getId() {
		return batchId;
	}
	
	public Direction getDirection() {
		return direction;
	}

	DatabaseTransactionDAO getDao() {
		return dao;
	}
}
