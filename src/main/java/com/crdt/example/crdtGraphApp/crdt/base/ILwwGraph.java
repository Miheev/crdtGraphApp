package com.crdt.example.crdtGraphApp.crdt.base;

import com.crdt.example.crdtGraphApp.crdt.LwwGraphNode;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface ILwwGraph<T> {
  boolean addNode(final LwwGraphNode<T> node);

  boolean mergeTree(final List<LwwGraphNode<T>> nodeList, final Map<UUID, Long> inputRemovedNodeMap);

  boolean removeNode(final UUID nodeId);

  boolean moveNode(final UUID destinationId, final LwwGraphNode<T> node);

  boolean isNodeExist(final UUID nodeID);

  List<LwwGraphNode<T>> findPath(final UUID startNodeId, final UUID endNodeId);

  Set<LwwGraphNode<T>> getConnectedNodeSet(final UUID nodeId);
}
