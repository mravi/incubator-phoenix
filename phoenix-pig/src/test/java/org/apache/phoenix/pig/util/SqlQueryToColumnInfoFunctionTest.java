package org.apache.phoenix.pig.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import org.apache.phoenix.pig.PhoenixPigConfiguration;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.ColumnInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

public class SqlQueryToColumnInfoFunctionTest  extends BaseConnectionlessQueryTest {

    private PhoenixPigConfiguration phoenixConfiguration;
    private Connection conn;
    private SqlQueryToColumnInfoFunction function;
    
    @Before
    public void setUp() throws SQLException {
        phoenixConfiguration = Mockito.mock(PhoenixPigConfiguration.class);
        conn = DriverManager.getConnection(getUrl());
        Mockito.when(phoenixConfiguration.getConnection()).thenReturn(conn);
        function = new SqlQueryToColumnInfoFunction(phoenixConfiguration);
    }
    
    @Test
    public void testValidSelectQuery() throws SQLException {
        String ddl = "CREATE TABLE EMPLOYEE " +
                "  (id integer not null, name varchar, age integer,location varchar " +
                "  CONSTRAINT pk PRIMARY KEY (id))\n";
        createTestTable(getUrl(), ddl);
  
        final String selectQuery = "SELECT name as a ,age AS b,UPPER(location) AS c, FROM EMPLOYEE";
        final ColumnInfo NAME_COLUMN = new ColumnInfo("A", Types.VARCHAR);
        final ColumnInfo AGE_COLUMN = new ColumnInfo("B", Types.INTEGER);
        final ColumnInfo LOCATION_COLUMN = new ColumnInfo("C", Types.VARCHAR);
        final List<ColumnInfo> expectedColumnInfos = ImmutableList.of(NAME_COLUMN, AGE_COLUMN,LOCATION_COLUMN);
        final List<ColumnInfo> actualColumnInfos = function.apply(selectQuery);
        Assert.assertEquals(expectedColumnInfos, actualColumnInfos);
        
    }
    
    @After
    public void tearDown() throws SQLException {
        conn.close();
    }

}
