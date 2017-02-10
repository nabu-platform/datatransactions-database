create table data_transactions (
	id varchar2(128) not null primary key,
	batch_id varchar2(128) not null,
	started timestamp not null,
	committed timestamp,
	done timestamp,
	state varchar2(128) not null,
	message varchar2(4000),
	provider_id varchar2(512) not null,
	request varchar2(1024),
	response varchar2(1024),
	transactionality varchar2(128) not null,
	-- in or out
	direction varchar2(128) not null,
	context varchar2(512),
	-- if this is an outgoing, you can link the id of the source (if any) to it
	source_id varchar2(512),
	-- who made the transaction, usually the server name (used for recovery)
	creator_id varchar2(512),
	-- the handler used for the result of the transaction
	handler_id varchar2(512),
	-- the type id of the properties for this data transaction, it is used to find the type again
	properties_type_id varchar2(512)
);

create table data_transaction_properties (
	data_transaction_id varchar2(128) not null,
	name varchar2(512) not null,
	value varchar2(4000) not null
);