package org.apache.samza.coordinator;

public interface GroupMemberChangeListener {
  void onGroupMemberChange (String memberIdentifier);
}
