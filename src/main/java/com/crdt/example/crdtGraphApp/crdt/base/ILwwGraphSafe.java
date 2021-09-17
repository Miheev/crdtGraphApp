package com.crdt.example.crdtGraphApp.crdt.base;

import com.crdt.example.crdtGraphApp.crdt.LwwGraphNode;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface ILwwGraphSafe<T> {
  boolean addNodeSafe(final LwwGraphNode<T> node);

  boolean mergeTreeSafe(final List<LwwGraphNode<T>> nodeList, final Map<UUID, Long> inputRemovedNodeMap);

  boolean removeNodeSafe(final UUID nodeId);

  boolean moveNodeSafe(final UUID destinationId, final LwwGraphNode<T> node);
}
