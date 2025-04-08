package io.ebean.datasource.test;

import java.util.Objects;

/**
 * @author Roland Praml, Foconis Analytics GmbH
 */
public class Db2Tenant {
  private final int id;
  private final String user;
  private final String password;
  private final String schema;

  public Db2Tenant(int id, String user, String password, String schema) {
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

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    Db2Tenant that = (Db2Tenant) o;
    return id == that.id;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id);
  }
}
