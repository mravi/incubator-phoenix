package org.apache.phoenix.pig.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.pig.PhoenixPigConfiguration;
import org.apache.phoenix.util.ColumnInfo;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public final class SqlQueryToColumnInfoFunction implements Function<String,List<ColumnInfo>> {
	
	private static final Log LOG = LogFactory.getLog(SqlQueryToColumnInfoFunction.class);
	private final PhoenixPigConfiguration phoenixConfiguration;

	public SqlQueryToColumnInfoFunction(
			final PhoenixPigConfiguration phoenixPigConfiguration) {
		super();
		this.phoenixConfiguration = phoenixPigConfiguration;
	}

	@Override
	public List<ColumnInfo> apply(String sqlQuery) {
		Preconditions.checkNotNull(sqlQuery);
		Connection connection = null;
		List<ColumnInfo> columnInfos = null;
        try {
            connection = this.phoenixConfiguration.getConnection();
            final Statement  statement = connection.createStatement();
            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
            final QueryPlan queryPlan = pstmt.compileQuery(sqlQuery);
            final List<? extends ColumnProjector> projectedColumns = queryPlan.getProjector().getColumnProjectors();
            columnInfos = Lists.newArrayListWithCapacity(projectedColumns.size());
            columnInfos = Lists.transform(projectedColumns, new Function<ColumnProjector,ColumnInfo>() {
            	@Override
				public ColumnInfo apply(final ColumnProjector columnProjector) {
					return new ColumnInfo(columnProjector.getName(), columnProjector.getExpression().getDataType().getSqlType());
				}
            	
            });
	   } catch (SQLException e) {
            LOG.error(String.format(" Error [%s] parsing SELECT query [%s] ",e.getMessage(),sqlQuery));
            Throwables.propagate(e);
        } finally {
            if(connection != null) {
                try {
                    connection.close();
                } catch(SQLException sqle) {
                    Throwables.propagate(sqle);
                }
            }
        }
		return columnInfos;
	}

}
