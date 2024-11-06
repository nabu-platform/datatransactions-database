/*
* Copyright (C) 2014 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

package be.nabu.libs.datatransactions.database;

import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import javax.sql.DataSource;

import be.nabu.libs.datatransactions.api.DataTransaction;
import be.nabu.libs.datatransactions.api.DataTransactionBatch;
import be.nabu.libs.datatransactions.api.DataTransactionHandle;
import be.nabu.libs.datatransactions.api.DataTransactionProvider;
import be.nabu.libs.datatransactions.api.Direction;
import be.nabu.libs.datatransactions.api.ProviderResolver;
import be.nabu.libs.datatransactions.api.Transactionality;

public class DatabaseTransactionProvider implements DataTransactionProvider {

	private DataSource dataSource;
	private TimeZone timezone;

	public DatabaseTransactionProvider(DataSource dataSource, TimeZone timezone) {
		this.dataSource = dataSource;
		this.timezone = timezone;
	}
	
	@Override
	public <T> DataTransactionBatch<T> newBatch(ProviderResolver<T> providerResolver, String context, String creatorId, String sourceId, String handlerId, Direction direction, Transactionality transactionality) {
		return new DatabaseTransactionBatch<T>(providerResolver, new DatabaseTransactionDAO(dataSource, timezone), context, creatorId, sourceId, handlerId, direction, transactionality);
	}
	
	@Override
	public DataTransaction<?> getTransaction(String transactionId) {
		try {
			return new DatabaseTransactionDAO(dataSource, timezone).getTransaction(transactionId);
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<DataTransaction<?>> getBatch(String batchId) {
		try {
			return new DatabaseTransactionDAO(dataSource, timezone).getBatch(batchId);
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<DataTransaction<?>> getPendingTransactions(String creatorId, Date from) {
		try {
			return new DatabaseTransactionDAO(dataSource, timezone).getPending(creatorId, from);
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public DataTransactionHandle getHandle(String transactionId) {
		return new DatabaseTransactionHandle(new DatabaseTransactionDAO(dataSource, timezone), (DatabaseTransaction<?>) getTransaction(transactionId));
	}

}
