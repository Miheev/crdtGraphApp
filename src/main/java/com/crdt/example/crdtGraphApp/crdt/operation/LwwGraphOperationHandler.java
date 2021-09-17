package com.crdt.example.crdtGraphApp.crdt.operation;

import com.crdt.example.crdtGraphApp.crdt.LwwGraphNode;
import lombok.extern.slf4j.Slf4j;

import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

@Slf4j
public class LwwGraphOperationHandler<T> {
  private final Map<UUID, Long> removedNodeMap;
  private final Map<UUID, LwwGraphNode<T>> nodeMap;
  private final Map<LwwGraphOperationEnum, Consumer<LwwGraphOperation<T>>> handlerMap =
    new EnumMap<>(LwwGraphOperationEnum.class);

  public LwwGraphOperationHandler(final Map<UUID, LwwGraphNode<T>> nodeMap, final Map<UUID, Long> removedNodeMap) {
    this.nodeMap = nodeMap;
    this.removedNodeMap = removedNodeMap;

    handlerMap.put(LwwGraphOperationEnum.ADD, this::addHandler);
    handlerMap.put(LwwGraphOperationEnum.MERGE, this::mergeHandler);
    handlerMap.put(LwwGraphOperationEnum.REMOVE, this::removeHandler);
  }

  public void apply(final LwwGraphOperation<T> operation) {
    log.debug("Operation processing, {}", operation);
    if (!operation.isOperationList() && isNodeRemoved(operation.getDataNode().getUid(), operation.getVersion())) {
      log.debug("node already removed, {}", operation);
      return;
    }
    if (!handlerMap.containsKey(operation.getOperationType())) {
      log.error("Operation not supported, {}", operation);
      return;
    }

    if (operation.isOperationList()) {
      mergeRemovedMap(operation.getRemovedNodeMap());
    }

    Consumer<LwwGraphOperation<T>> consumer = handlerMap.get(operation.getOperationType());
    consumer.accept(operation);
    log.debug("Operation completed, {}", operation);
  }

  private void addHandler(final LwwGraphOperation<T> operation) {
    addNode(operation.getDataNode(), operation.getVersion());
  }

  private void mergeHandler(final LwwGraphOperation<T> operation) {
    for (LwwGraphNode<T> nodeToAdd : operation.getDataNodeList()) {
      if (isNodeRemoved(nodeToAdd.getUid(), nodeToAdd.getVersion())) {
        log.debug("node already removed, {}", operation);
        continue;
      }
      addNode(nodeToAdd, nodeToAdd.getVersion());
    }
  }

  private void addNode(final LwwGraphNode<T> nodeToAdd, final long version) {
    if (!nodeMap.containsKey(nodeToAdd.getUid())) {
      processNodeAddition(nodeToAdd, version);
      return;
    }

    LwwGraphNode<T> localNode = nodeMap.get(nodeToAdd.getUid());
    if (localNode.getVersion() > version) {
      log.debug("Node not updated, local version is newer: \nlocalNode {}, \nversion {} \nnodeToAdd {}",
        localNode, version, nodeToAdd);
      return;
    }
    if (localNode.getVersion() < version) {
      processNodeAddition(nodeToAdd, version);
      return;
    }

    // @TODO: we use nanosecond to escape such condition, in case of very high volume of data, we need to define policy what todo with two nodes with equal timestamp: accept or reject changes
    log.error("Condition not supported: operation.getVersion() == localNode.getVersion(), version {}, nodeToAdd {}",
      version, nodeToAdd);
  }

  private void processNodeAddition(final LwwGraphNode<T> nodeToAdd, final long version) {
    nodeToAdd.setVersion(version);
    nodeMap.put(nodeToAdd.getUid(), nodeToAdd);
    linkNode(nodeToAdd);
  }

  private void mergeRemovedMap(final Map<UUID, Long> inoutRemovedNodeMap) {
    for (Map.Entry<UUID, Long> removedEntry : inoutRemovedNodeMap.entrySet()) {
      if (!nodeMap.containsKey(removedEntry.getKey())) {
        removedNodeMap.put(removedEntry.getKey(), removedEntry.getValue());
        continue;
      }

      LwwGraphNode<T> localNode = nodeMap.get(removedEntry.getKey());
      if (localNode.getVersion() < removedEntry.getValue()) {
        removedNodeMap.put(removedEntry.getKey(), removedEntry.getValue());
        removeNode(localNode);
        continue;
      }
      if (localNode.getVersion() == removedEntry.getValue()) {
        // @TODO: we use nanosecond to escape such condition, in case of very high volume of data, we need to define policy what todo with two nodes with equal timestamp: accept or reject changes
        log.error("Condition not supported: localNode.getVersion() == removedEntry.getValue(), removedEntry {}",
          removedEntry);
      }
    }
  }

  private void removeHandler(final LwwGraphOperation<T> operation) {
    removedNodeMap.put(operation.getDataNode().getUid(), operation.getVersion());
    removeNode(operation.getDataNode());
  }

  private boolean isNodeRemoved(final UUID nodeId, final long operationVersion) {
    if (!removedNodeMap.containsKey(nodeId)) {
      return false;
    }

    Long removedVersion = removedNodeMap.get(nodeId);
    if (operationVersion < removedVersion) {
      return true;
    }
    if (operationVersion > removedVersion) {
      return false;
    }

    // @TODO: we use nanosecond to escape such condition, in case of very high volume of data, we need to define policy what todo with two nodes with equal timestamp: accept or reject changes
    log.error("Condition not supported: operation.getVersion() == removedVersion, operation version {}, nodeId {}",
      operationVersion, nodeId);
    return true;
  }


  /**
   * Simple removal operation
   * Since we have back reference,
   * we need to clean references to prevent memory leaks for child tree with removed parent
   */
  private void removeNode(final LwwGraphNode<T> node) {
    for (UUID parentId : node.getParentSet()) {
      nodeMap.get(parentId).removeChild(node);
    }
    node.getParentSet().clear();

    for (UUID childId : node.getChildSet()) {
      nodeMap.get(childId).removeParent(node);
    }
    node.getChildSet().clear();

    nodeMap.remove(node.getUid());
  }

  private void linkNode(final LwwGraphNode<T> node) {
    LwwGraphNode<T> linkNode;
    UUID linkId;

    // Link parents
    Iterator<UUID> iterator = node.getParentSet().iterator();
    while (iterator.hasNext()) {
      linkId = iterator.next();
      if (nodeMap.containsKey(linkId)) {
        linkNode = nodeMap.get(linkId);
        linkNode.addChildAndParentRef(node);
      } else {
        log.warn("Parent node not found. Link can not be added. parentId=${}", linkId);
        iterator.remove();
      }
    }

    // Link children
    iterator = node.getChildSet().iterator();
    while (iterator.hasNext()) {
      linkId = iterator.next();
      if (nodeMap.containsKey(linkId)) {
        linkNode = nodeMap.get(linkId);
        node.addChildAndParentRef(linkNode);
      } else {
        log.warn("Child node not found. Link can not be added. childId=${}", linkId);
        iterator.remove();
      }
    }
  }
}
