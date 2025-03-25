package io.ebean.datasource.tcdriver;

/**
 * @author Roland Praml, Foconis Analytics GmbH
 */
public class TrustedContextTenant {
  private final int id;
  private final String user;
  private final String password;
  private final String schema;

  public TrustedContextTenant(int id, String user, String password, String schema) {
    this.id = id;
    this.user = user;
    this.password = password;
    this.schema = schema;
  }

  public int id() {
    return id;
  }

  public String user() {
    return user;
  }

  public String password() {
    return password;
  }

  public String schema() {
    return schema;
  }

}
