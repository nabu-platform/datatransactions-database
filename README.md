# Database Data Transactions

This package provides a database-backed implementation of the data transactions api.

Depending on the transactionality, the following actions are performed:

- THREE_PHASE: one insert and two updates
- TWO_PHASE: one insert and one update
- ONE_PHASE: one insert