package com.crdt.example.crdtGraphApp.crdt;

import com.crdt.example.crdtGraphApp.crdt.base.ILwwGraphSafe;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.security.InvalidParameterException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@NoArgsConstructor
public class LwwGraphSafe<T> extends LwwGraph<T> implements ILwwGraphSafe<T> {
  @Override
  public boolean addNodeSafe(final LwwGraphNode<T> node) {
    if (node == null) {
      throw new InvalidParameterException("node should not be null");
    }

    return addNode(node);
  }

  @Override
  public boolean mergeTreeSafe(final List<LwwGraphNode<T>> nodeList, final Map<UUID, Long> inputRemovedNodeMap) {
    if (nodeList == null) {
      throw new InvalidParameterException("nodeSet should not be null");
    }
    if (inputRemovedNodeMap == null) {
      throw new InvalidParameterException("removedNodeSet should not be null");
    }
    return mergeTree(nodeList, inputRemovedNodeMap);
  }

  @Override
  public boolean removeNodeSafe(final UUID nodeId) {
    if (!isNodeExist(nodeId)) {
      throw new InvalidParameterException(String.format("node %s already deleted", nodeId));
    }
    return removeNode(nodeId);
  }

  @Override
  public boolean moveNodeSafe(final UUID destinationId, final LwwGraphNode<T> node) {
    return moveNode(destinationId, node);
  }
}
