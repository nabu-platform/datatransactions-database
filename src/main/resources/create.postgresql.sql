create table data_transactions (
	id varchar not null primary key,
	batch_id varchar not null,
	started timestamp not null,
	committed timestamp,
	done timestamp,
	state varchar not null,
	message varchar,
	provider_id varchar not null,
	request varchar,
	response varchar,
	transactionality varchar not null,
	-- in or out
	direction varchar not null,
	context varchar,
	-- if this is an outgoing, you can link the id of the source (if any) to it
	source_id varchar,
	-- who made the transaction, usually the server name (used for recovery)
	creator_id varchar,
	-- the handler used for the result of the transaction
	handler_id varchar,
	-- the type id of the properties for this data transaction, it is used to find the type again
	properties_type_id varchar
);

create table data_transaction_properties (
	data_transaction_id varchar not null,
	name varchar not null,
	value varchar not null
);