package be.nabu.libs.datatransactions.database;

import java.net.URI;
import java.util.Date;

import be.nabu.libs.datatransactions.api.DataTransaction;
import be.nabu.libs.datatransactions.api.DataTransactionState;
import be.nabu.libs.datatransactions.api.Direction;
import be.nabu.libs.datatransactions.api.Transactionality;

public class DatabaseTransaction<T> implements DataTransaction<T> {

	private String providerId;
	private T properties;
	private URI request, response;
	private Date started, committed, done;
	private DataTransactionState state;
	private String message;
	private String id;
	private Direction direction;
	private String sourceId, creatorId;
	private String context;
	private String batchId;
	private Transactionality transactionality;

	@Override
	public T getProperties() {
		return properties;
	}
	void setProperties(T properties) {
		this.properties = properties;
	}

	@Override
	public URI getRequest() {
		return request;
	}

	@Override
	public URI getResponse() {
		return response;
	}

	void setResponse(URI response) {
		this.response = response;
	}

	@Override
	public Date getStarted() {
		return started;
	}

	@Override
	public DataTransactionState getState() {
		return state;
	}

	void setStarted(Date started) {
		this.started = started;
	}

	void setState(DataTransactionState state) {
		this.state = state;
	}

	@Override
	public String getMessage() {
		return message;
	}

	void setMessage(String message) {
		this.message = message;
	}

	@Override
	public String getId() {
		return id;
	}

	void setId(String id) {
		this.id = id;
	}

	public Direction getDirection() {
		return direction;
	}

	@Override
	public String getBatchId() {
		return batchId;
	}

	@Override
	public String getContext() {
		return context;
	}

	@Override
	public String getProviderId() {
		return providerId;
	}

	@Override
	public String getSourceId() {
		return sourceId;
	}

	@Override
	public String getCreatorId() {
		return creatorId;
	}

	@Override
	public Date getCommitted() {
		return committed;
	}

	@Override
	public Date getDone() {
		return done;
	}

	@Override
	public Transactionality getTransactionality() {
		return transactionality;
	}

	void setProviderId(String providerId) {
		this.providerId = providerId;
	}

	void setRequest(URI request) {
		this.request = request;
	}

	void setCommitted(Date committed) {
		this.committed = committed;
	}

	void setDone(Date done) {
		this.done = done;
	}

	void setDirection(Direction direction) {
		this.direction = direction;
	}

	void setSourceId(String sourceId) {
		this.sourceId = sourceId;
	}

	void setCreatorId(String creatorId) {
		this.creatorId = creatorId;
	}

	void setContext(String context) {
		this.context = context;
	}

	void setBatchId(String batchId) {
		this.batchId = batchId;
	}

	void setTransactionality(Transactionality transactionality) {
		this.transactionality = transactionality;
	}
}
