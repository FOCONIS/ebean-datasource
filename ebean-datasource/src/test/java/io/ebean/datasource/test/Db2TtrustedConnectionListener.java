package io.ebean.datasource.test;

import io.ebean.datasource.DataSourceConnection;
import io.ebean.datasource.DataSourcePool;
import io.ebean.datasource.DataSourcePoolListener;

import java.sql.SQLException;
import java.util.concurrent.atomic.LongAdder;

/**
 * Listener, that sets up TrustedConnection properly
 *
 * @author Roland Praml, Foconis Analytics GmbH
 */

public class Db2TtrustedConnectionListener implements DataSourcePoolListener {

  private final LongAdder switchCount;

  public Db2TtrustedConnectionListener(LongAdder switchCount) {
    this.switchCount = switchCount;
  }

  @Override
  public void onAfterBorrowConnection(DataSourcePool pool, DataSourceConnection connection) throws SQLException {
    Db2Tenant tenant = (Db2Tenant) connection.affinityId();
    Db2TrustedConnection trustedDb2Connection = connection.unwrap(Db2TrustedConnection.class);

    String user = tenant == null ? null : tenant.user();
    String password = tenant == null ? null : tenant.password();
    String schema = tenant == null ? null : tenant.schema();
    if (trustedDb2Connection.switchUser(user, password)) {
      trustedDb2Connection.setSchema(schema);
      connection.clearPreparedStatementCache();
      if (!trustedDb2Connection.isValid(1)) {
        throw new SQLException("Connection is invalid");
      }
      switchCount.increment();
    }
  }


}
