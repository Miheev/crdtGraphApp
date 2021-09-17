package com.crdt.example.crdtGraphApp.crdt;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@Data
@RequiredArgsConstructor
@AllArgsConstructor
public final class LwwGraphNode<T> {
  private final UUID uid;
  private T value;
  private long version = 0;
  private Set<UUID> childSet = new HashSet<>();
  private Set<UUID> parentSet = new HashSet<>();

  public LwwGraphNode(final UUID uid, final T value) {
    this(uid);
    this.value = value;
  }

  public LwwGraphNode(final UUID uid, final T value, final long version) {
    this(uid, value);
    this.version = version;
  }

  public void addChildAndParentRef(final LwwGraphNode<T> node) {
    childSet.add(node.getUid());
    node.addParent(this);
  }

  public void removeChild(final LwwGraphNode<T> node) {
    childSet.remove(node.getUid());
  }

  public void addParent(final LwwGraphNode<T> node) {
    parentSet.add(node.getUid());
  }

  public void removeParent(final LwwGraphNode<T> node) {
    parentSet.remove(node.getUid());
  }

  public LwwGraphNode<T> copy() {
    return new LwwGraphNode<T>(uid, value, version, childSet, parentSet);
  }
}
