using Microsoft.Extensions.Logging;
using RepositoryPattern.Abstractions;
using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace RepositoryPattern.SqlServer
{
    public class SQLServerConnectionFactory : IMssqlFactory
    {
        private readonly DbConnectionStringBuilder _connectionBuilder;
        private readonly ILogger<SQLServerConnectionFactory> _logger;
        private SqlConnection? _connection;
        private bool _committed;

        public SQLServerConnectionFactory(ILoggerFactory loggerFactory, string connectionString)
        {
            _logger = loggerFactory.CreateLogger<SQLServerConnectionFactory>();
            try
            {
                _connectionBuilder = new DbConnectionStringBuilder
                {
                    ConnectionString = connectionString
                };
            }
            catch (Exception ex)
            {
                throw new DataRepositoryException(DataRepositoryException.INVALID_CONNECTION_STRING, ex);
            }
            _logger.LogTrace("Valid connection string retrieved from DB");
        }

        public override async Task<IDbConnection> CreateOpenConnectionAsync()
        {
            if (_connection is null)
            {
                _logger.LogDebug("Initiating connection");
                _connection = new SqlConnection(_connectionBuilder.ConnectionString);
            }

            if (_connection.State != ConnectionState.Open)
            {
                _logger.LogDebug("Connection hasn't opened yet or it was closed");
                await _connection.OpenAsync();
            }

            return _connection;
        }


        public bool IsDeadlockException(Exception ex)
        {
            return ex != null
&& (ex is DbException
                && ex.Message.Contains("deadlock")
|| (ex.InnerException != null
&& IsDeadlockException(ex.InnerException)));

        }

        // Flag: Has Dispose already been called?
        bool disposed = false;

        // Public implementation of Dispose pattern callable by consumers.
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        // Protected implementation of Dispose pattern.
        protected virtual void Dispose(bool disposing)
        {
            if (disposed)
                return;

            if (disposing)
            {
                if (_connection is object)
                {
                    if (CurrentTransaction is object)
                    {
                        if (!_committed)
                            CurrentTransaction.Rollback();
                        CurrentTransaction.Dispose();
                    }
                    _connection.Dispose();
                }
            }
            disposed = true;
        }

        protected override async Task Initiate(IsolationLevel isolationLevel)
        {
            var connection = await CreateOpenConnectionAsync();
            CurrentTransaction = connection.BeginTransaction(isolationLevel);
        }

        protected override Task Commit()
        {
            if (CurrentTransaction is object)
            {
                CurrentTransaction.Commit();
                _committed = true;
            }
            return Task.CompletedTask;
        }

        ~SQLServerConnectionFactory()
        {
            Dispose(false);
        }
    }
}
