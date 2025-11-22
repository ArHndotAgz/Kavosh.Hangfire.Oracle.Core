using System;
using System.Data;
using System.Threading;
using Dapper;
using Hangfire.Logging;

namespace Hangfire.Oracle.Core
{
    public class OracleDistributedLock : IDisposable, IComparable
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(OracleDistributedLock));
        private readonly TimeSpan _timeout;
        private readonly OracleStorage _storage;
        private readonly DateTime _start;
        private readonly CancellationToken _cancellationToken;
        private readonly IDbConnection _connection;
        private readonly bool _ownsConnection;

        private const int DelayBetweenPasses = 100;

        public OracleDistributedLock(OracleStorage storage, string resource, TimeSpan timeout)
        {
            Logger.TraceFormat("OracleDistributedLock resource={0}, timeout={1}", resource, timeout);
            
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _connection = storage.CreateAndOpenConnection();
            Resource = resource;
            _timeout = timeout;
            _cancellationToken = default;
            _start = DateTime.UtcNow;
            _ownsConnection = true;
        }

        public OracleDistributedLock(IDbConnection connection, string resource, TimeSpan timeout)
            : this(connection, resource, timeout, new CancellationToken())
        {
        }

        public OracleDistributedLock(IDbConnection connection, string resource, TimeSpan timeout, CancellationToken cancellationToken)
        {
            Logger.TraceFormat("OracleDistributedLock resource={0}, timeout={1}", resource, timeout);

            if (connection == null) throw new ArgumentNullException(nameof(connection));
            
            _storage = ExtractStorageFromConnection(connection);
            _connection = connection;
            Resource = resource;
            _timeout = timeout;
            _cancellationToken = cancellationToken;
            _start = DateTime.UtcNow;
            _ownsConnection = false;
        }

        public string Resource { get; }

        private OracleStorage ExtractStorageFromConnection(IDbConnection connection)
        {
            var storage = OracleStorageConnectionContext.Current;
            if (storage == null)
            {
                Logger.Warn("Cannot extract OracleStorage from connection context. Using default table names.");
            }
            return storage;
        }

        private string GetDistributedLockTable()
        {
            var tableName = _storage?.TableNameProvider?.GetTableName("DistributedLock") ?? "HF_DISTRIBUTED_LOCK";
            Logger.DebugFormat("GetDistributedLockTable resolved to: {0}", tableName);
            return tableName;
        }

        private int AcquireLock(string resource, TimeSpan timeout)
        {
            var tableName = GetDistributedLockTable();
            
            if (string.IsNullOrWhiteSpace(tableName))
            {
                throw new InvalidOperationException("DistributedLock table name is empty!");
            }

            var sql = $@"INSERT INTO {tableName} (""RESOURCE"", CREATED_AT)
                         SELECT :RES, :NOW
                         FROM DUAL
                         WHERE NOT EXISTS (
                             SELECT 1 FROM {tableName}
                             WHERE ""RESOURCE"" = :RES
                               AND CREATED_AT > :EXPIRED
                         )";

            Logger.DebugFormat("AcquireLock SQL:\n{0}", sql);

            return _connection.Execute(
                sql,
                new {
                    RES = resource,
                    NOW = DateTime.UtcNow,
                    EXPIRED = DateTime.UtcNow.Add(timeout.Negate())
                });
        }

        public void Dispose()
        {
            Release();

            if (_ownsConnection)
            {
                _storage?.ReleaseConnection(_connection);
            }
        }

        internal OracleDistributedLock Acquire()
        {
            Logger.TraceFormat("Acquire resource={0}, timeout={1}", Resource, _timeout);

            int insertedObjectCount;
            do
            {
                _cancellationToken.ThrowIfCancellationRequested();

                insertedObjectCount = AcquireLock(Resource, _timeout);

                if (ContinueCondition(insertedObjectCount))
                {
                    _cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                    _cancellationToken.ThrowIfCancellationRequested();
                }
            } while (ContinueCondition(insertedObjectCount));

            if (insertedObjectCount == 0)
            {
                throw new OracleDistributedLockException("cannot acquire lock");
            }
            return this;
        }

        private bool ContinueCondition(int insertedObjectCount)
        {
            return insertedObjectCount == 0 && _start.Add(_timeout) > DateTime.UtcNow;
        }

        internal void Release()
        {
            Logger.TraceFormat("Release resource={0}", Resource);

            var tableName = GetDistributedLockTable();
            if (string.IsNullOrWhiteSpace(tableName))
            {
                Logger.Error("DistributedLock table name is empty during Release!");
                return;
            }

            var sql = $@"DELETE FROM {tableName} WHERE ""RESOURCE"" = :RES";
            
            Logger.DebugFormat("Release SQL:\n{0}", sql);
            
            _connection.Execute(sql, new { RES = Resource });
        }

        public int CompareTo(object obj)
        {
            if (obj == null)
            {
                return 1;
            }

            if (obj is OracleDistributedLock oracleDistributedLock)
            {
                return string.Compare(Resource, oracleDistributedLock.Resource, StringComparison.OrdinalIgnoreCase);
            }

            throw new ArgumentException("Object is not a OracleDistributedLock");
        }
    }

    // Thread-local context to pass storage through connection calls
    internal static class OracleStorageConnectionContext
    {
        [ThreadStatic]
        private static OracleStorage _current;

        public static OracleStorage Current
        {
            get => _current;
            set => _current = value;
        }
    }
}