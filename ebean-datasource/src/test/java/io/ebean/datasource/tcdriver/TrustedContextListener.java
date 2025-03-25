package io.ebean.datasource.tcdriver;

import io.ebean.datasource.DataSourceConnection;
import io.ebean.datasource.DataSourcePool;
import io.ebean.datasource.DataSourcePoolListener;

import java.sql.SQLException;

/**
 * Listener, that sets up TrustedConnection properly
 *
 * @author Roland Praml, Foconis Analytics GmbH
 */
public class TrustedContextListener implements DataSourcePoolListener {

    @Override
    public void onAfterBorrowConnection(DataSourcePool pool, DataSourceConnection connection) throws SQLException {
        TrustedContextTenant tenant = (TrustedContextTenant) connection.affinityId();
        TrustedDb2Connection trustedDb2Connection = connection.unwrap(TrustedDb2Connection.class);
        if (trustedDb2Connection.switchUser(tenant.user(), tenant.password())) {
            trustedDb2Connection.setSchema(tenant.schema());
            connection.clearPreparedStatementCache();
        }
    }


}
