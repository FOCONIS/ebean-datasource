package io.ebean.datasource.tcdriver;

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

public class TrustedContextListener implements DataSourcePoolListener {

  private final LongAdder switchCount;

  public TrustedContextListener(LongAdder switchCount) {
    this.switchCount = switchCount;
  }

  @Override
  public void onAfterBorrowConnection(DataSourcePool pool, DataSourceConnection connection) throws SQLException {
    TrustedContextTenant tenant = (TrustedContextTenant) connection.affinityId();
    TrustedDb2Connection trustedDb2Connection = connection.unwrap(TrustedDb2Connection.class);

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
