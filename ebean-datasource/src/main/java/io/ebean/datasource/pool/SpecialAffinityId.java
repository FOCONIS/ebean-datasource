package io.ebean.datasource.pool;

/**
 * These are special affinity ids, when requesting connections for heartbeat or reconfiguration.
 *
 * @author Roland Praml, Foconis Analytics GmbH
 */
enum SpecialAffinityId {
  HEARTBEAT, RECONFIGURE
}
