package liquibase.executor.jvm;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.database.PreparedStatementFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.executor.AbstractExecutor;
import liquibase.executor.Executor;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.sql.visitor.SqlVisitor;
import liquibase.statement.ExecutablePreparedStatement;
import liquibase.statement.SqlStatement;
import liquibase.util.JdbcUtils;
import liquibase.util.StringUtils;

public class JdbcExecutor extends AbstractExecutor implements Executor {
    private Logger log = LogFactory.getLogger();

    public boolean updatesDatabase() {
        return true;
    }

    public Object execute(StatementCallback action, List<SqlVisitor> sqlVisitors) throws DatabaseException {
        DatabaseConnection con = this.database.getConnection();
        Statement stmt = null;
        try {
            if (con instanceof liquibase.database.OfflineConnection)
                throw new DatabaseException("Cannot execute commands against an offline database");
            stmt = ((JdbcConnection)con).getUnderlyingConnection().createStatement();
            Statement stmtToUse = stmt;
            return action.doInStatement(stmtToUse);
        } catch (SQLException ex) {
            JdbcUtils.closeStatement(stmt);
            stmt = null;
            throw new DatabaseException("Error executing SQL " + StringUtils.join(applyVisitors(action.getStatement(), sqlVisitors), "; on " + con.getURL()) + ": " + ex.getMessage(), ex);
        } finally {
            JdbcUtils.closeStatement(stmt);
        }
    }

    public Object execute(CallableStatementCallback action, List<SqlVisitor> sqlVisitors) throws DatabaseException {
        DatabaseConnection con = this.database.getConnection();
        if (con instanceof liquibase.database.OfflineConnection)
            throw new DatabaseException("Cannot execute commands against an offline database");
        CallableStatement stmt = null;
        try {
            String sql = applyVisitors(action.getStatement(), sqlVisitors)[0];
            stmt = ((JdbcConnection)con).getUnderlyingConnection().prepareCall(sql);
            return action.doInCallableStatement(stmt);
        } catch (SQLException ex) {
            JdbcUtils.closeStatement(stmt);
            stmt = null;
            throw new DatabaseException("Error executing SQL " + StringUtils.join(applyVisitors(action.getStatement(), sqlVisitors), "; on " + con.getURL()) + ": " + ex.getMessage(), ex);
        } finally {
            JdbcUtils.closeStatement(stmt);
        }
    }

    public void execute(SqlStatement sql) throws DatabaseException {
        execute(sql, new ArrayList<SqlVisitor>());
    }

    public void execute(SqlStatement sql, List<SqlVisitor> sqlVisitors) throws DatabaseException {
        if (sql instanceof ExecutablePreparedStatement) {
            ((ExecutablePreparedStatement)sql).execute(new PreparedStatementFactory((JdbcConnection)this.database.getConnection()));
            return;
        }
        execute(new ExecuteStatementCallback(sql, sqlVisitors), sqlVisitors);
    }

    public Object query(SqlStatement sql, ResultSetExtractor rse) throws DatabaseException {
        return query(sql, rse, new ArrayList<SqlVisitor>());
    }

    public Object query(SqlStatement sql, ResultSetExtractor rse, List<SqlVisitor> sqlVisitors) throws DatabaseException {
        if (sql instanceof liquibase.statement.CallableSqlStatement)
            return execute(new QueryCallableStatementCallback(sql, rse), sqlVisitors);
        return execute(new QueryStatementCallback(sql, rse, sqlVisitors), sqlVisitors);
    }

    public List query(SqlStatement sql, RowMapper rowMapper) throws DatabaseException {
        return query(sql, rowMapper, new ArrayList<SqlVisitor>());
    }

    public List query(SqlStatement sql, RowMapper rowMapper, List<SqlVisitor> sqlVisitors) throws DatabaseException {
        return (List)query(sql, new RowMapperResultSetExtractor(rowMapper), sqlVisitors);
    }

    public Object queryForObject(SqlStatement sql, RowMapper rowMapper) throws DatabaseException {
        return queryForObject(sql, rowMapper, new ArrayList<SqlVisitor>());
    }

    public Object queryForObject(SqlStatement sql, RowMapper rowMapper, List<SqlVisitor> sqlVisitors) throws DatabaseException {
        List results = query(sql, rowMapper, sqlVisitors);
        return JdbcUtils.requiredSingleResult(results);
    }

    public <T> T queryForObject(SqlStatement sql, Class<T> requiredType) throws DatabaseException {
        return queryForObject(sql, requiredType, new ArrayList<SqlVisitor>());
    }

    public <T> T queryForObject(SqlStatement sql, Class<T> requiredType, List<SqlVisitor> sqlVisitors) throws DatabaseException {
        return (T)queryForObject(sql, getSingleColumnRowMapper(requiredType), sqlVisitors);
    }

    public long queryForLong(SqlStatement sql) throws DatabaseException {
        return queryForLong(sql, new ArrayList<SqlVisitor>());
    }

    public long queryForLong(SqlStatement sql, List<SqlVisitor> sqlVisitors) throws DatabaseException {
        Number number = (Number) queryForObject(sql, (Class)Long.class, sqlVisitors);
        return (number != null) ? number.longValue() : 0L;
    }

    public int queryForInt(SqlStatement sql) throws DatabaseException {
        return queryForInt(sql, new ArrayList<SqlVisitor>());
    }

    public int queryForInt(SqlStatement sql, List<SqlVisitor> sqlVisitors) throws DatabaseException {
        Number number = (Number) queryForObject(sql, (Class)Integer.class, sqlVisitors);
        return (number != null) ? number.intValue() : 0;
    }

    public List queryForList(SqlStatement sql, Class elementType) throws DatabaseException {
        return queryForList(sql, elementType, new ArrayList<SqlVisitor>());
    }

    public List queryForList(SqlStatement sql, Class elementType, List<SqlVisitor> sqlVisitors) throws DatabaseException {
        return query(sql, getSingleColumnRowMapper(elementType), sqlVisitors);
    }

    public List<Map<String, ?>> queryForList(SqlStatement sql) throws DatabaseException {
        return queryForList(sql, new ArrayList<SqlVisitor>());
    }

    public List<Map<String, ?>> queryForList(SqlStatement sql, List<SqlVisitor> sqlVisitors) throws DatabaseException {
        return query(sql, getColumnMapRowMapper(), sqlVisitors);
    }

    public int update(SqlStatement sql) throws DatabaseException {
        return update(sql, new ArrayList<SqlVisitor>());
    }

    public int update(final SqlStatement sql, final List<SqlVisitor> sqlVisitors) throws DatabaseException {
        if (sql instanceof liquibase.statement.CallableSqlStatement)
            throw new DatabaseException("Direct update using CallableSqlStatement not currently implemented");
        class UpdateStatementCallback implements StatementCallback {
            public Object doInStatement(Statement stmt) throws SQLException, DatabaseException {
                String[] sqlToExecute = JdbcExecutor.this.applyVisitors(sql, sqlVisitors);
                if (sqlToExecute.length != 1)
                    throw new DatabaseException("Cannot call update on Statement that returns back multiple Sql objects");
                JdbcExecutor.this.log.debug("Executing UPDATE database command: " + sqlToExecute[0]);
                return Integer.valueOf(stmt.executeUpdate(sqlToExecute[0]));
            }

            public SqlStatement getStatement() {
                return sql;
            }
        };
        return ((Integer)execute(new UpdateStatementCallback(), sqlVisitors)).intValue();
    }

    protected RowMapper getColumnMapRowMapper() {
        return new ColumnMapRowMapper();
    }

    protected RowMapper getSingleColumnRowMapper(Class requiredType) {
        return new SingleColumnRowMapper(requiredType);
    }

    public void comment(String message) throws DatabaseException {
        LogFactory.getLogger().debug(message);
    }

    protected void prepare() throws SQLException {
        if (this.database instanceof liquibase.database.core.PostgresDatabase) {
            Statement stmt = ((JdbcConnection)this.database.getConnection()).getUnderlyingConnection().createStatement();
            stmt.execute("SET SCHEMA '" + this.database.getDefaultSchemaName() + "'");
        }
    }

    private static class RowCallbackHandlerResultSetExtractor implements ResultSetExtractor {
        private final RowCallbackHandler rch;

        public RowCallbackHandlerResultSetExtractor(RowCallbackHandler rch) {
            this.rch = rch;
        }

        public Object extractData(ResultSet rs) throws SQLException {
            while (rs.next())
                this.rch.processRow(rs);
            return null;
        }
    }

    private class ExecuteStatementCallback implements StatementCallback {
        private final SqlStatement sql;

        private final List<SqlVisitor> sqlVisitors;

        private ExecuteStatementCallback(SqlStatement sql, List<SqlVisitor> sqlVisitors) {
            this.sql = sql;
            this.sqlVisitors = sqlVisitors;
        }

        public Object doInStatement(Statement stmt) throws SQLException, DatabaseException {
            for (String statement : JdbcExecutor.this.applyVisitors(this.sql, this.sqlVisitors)) {
                if (JdbcExecutor.this.database instanceof liquibase.database.core.OracleDatabase)
                    statement = statement.replaceFirst("/\\s*/\\s*$", "");
                JdbcExecutor.this.log.debug("Executing EXECUTE database command: " + statement);
                if (statement.contains("?"))
                    stmt.setEscapeProcessing(false);
                try {
                    JdbcExecutor.this.prepare();
                    stmt.execute(statement);
                } catch (SQLException e) {
                    throw e;
                }
            }
            return null;
        }

        public SqlStatement getStatement() {
            return this.sql;
        }
    }

    private class QueryStatementCallback implements StatementCallback {
        private final SqlStatement sql;

        private final List<SqlVisitor> sqlVisitors;

        private final ResultSetExtractor rse;

        private QueryStatementCallback(SqlStatement sql, ResultSetExtractor rse, List<SqlVisitor> sqlVisitors) {
            this.sql = sql;
            this.rse = rse;
            this.sqlVisitors = sqlVisitors;
        }

        public Object doInStatement(Statement stmt) throws SQLException, DatabaseException {
            ResultSet rs = null;
            try {
                String[] sqlToExecute = JdbcExecutor.this.applyVisitors(this.sql, this.sqlVisitors);
                if (sqlToExecute.length != 1)
                    throw new DatabaseException("Can only query with statements that return one sql statement");
                JdbcExecutor.this.log.debug("Executing QUERY database command: " + sqlToExecute[0]);
                JdbcExecutor.this.prepare();
                rs = stmt.executeQuery(sqlToExecute[0]);
                ResultSet rsToUse = rs;
                return this.rse.extractData(rsToUse);
            } finally {
                JdbcUtils.closeResultSet(rs);
            }
        }

        public SqlStatement getStatement() {
            return this.sql;
        }
    }

    private class QueryCallableStatementCallback implements CallableStatementCallback {
        private final SqlStatement sql;

        private final ResultSetExtractor rse;

        private QueryCallableStatementCallback(SqlStatement sql, ResultSetExtractor rse) {
            this.sql = sql;
            this.rse = rse;
        }

        public Object doInCallableStatement(CallableStatement cs) throws SQLException, DatabaseException {
            ResultSet rs = null;
            try {
                JdbcExecutor.this.prepare();
                rs = cs.executeQuery();
                return this.rse.extractData(rs);
            } finally {
                JdbcUtils.closeResultSet(rs);
            }
        }

        public SqlStatement getStatement() {
            return this.sql;
        }
    }
}
