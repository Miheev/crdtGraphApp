package com.crdt.example.crdtGraphApp.crdt;

import com.crdt.example.crdtGraphApp.crdt.base.ILwwGraph;
import com.crdt.example.crdtGraphApp.crdt.operation.LwwGraphOperation;
import com.crdt.example.crdtGraphApp.crdt.operation.LwwGraphOperationEnum;
import com.crdt.example.crdtGraphApp.crdt.operation.LwwGraphOperationHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * This class implement LWW Element-like Graph, which support non-blocking writes.
 * <p>
 * Assume vertex is node (instance of LwwGraphNode).
 * Vertex can have any number parents and children.
 * Edge is node with one parent and one child vertex.
 * <p>
 * Core components:
 * - graph (set of nodes + map of connections per IDs)
 * - set of deleted nodes IDs
 * - operation queue ordered by time of operation addition to queue
 * - queue handler, which apply operation to graph
 * <p>
 * For achieve non blocking behavior System.nanoTime is used during adding to queue,
 * keep in mind potential clock synchronization problem during merging of different class instances
 * from different server nodes.
 * <p>
 * Implemented strategy: one queue handler for multiple non-blocking writers.
 * <p>
 * Queue handler starts to execute incoming change requests from queue from first available thread,
 * which have produced change request.
 * Handler continue execution until queue will be empty, then with new change request it can start from another thread.
 * <p>
 * The simplest way to implement multiple handlers for multiple writers scenario is extend this class in such way,
 * which allow to create one LwwGraph main class, which includes main ordered operation queue and graph.
 * And from other side, approach should allow to create more instances of this class
 * for split handler's workload between them, then synchronize such instances with main instance.
 * <p>
 * More detailed thoughts:
 * - main LwwGraph instance receive non-blocking writes requests to queue (no changes here)
 * - assign request execution to one of the sub-graphs (another instances of LwwGraph),
 * it means delegate execution of change request from main queue to sub-queue
 * with own handler just for this sub-queue (original LwwGraph implementation)
 * - use load balancing algorithm like Round-Robin or more progressive one to delegate requests from queue to sub-queue
 * - set limit of operations executed in sub-graphs
 * - synchronize main graph with sub-graph when limit reached by adding merge operation to main queue,
 * which will be executed by main instance queue handler directly
 * - when limit reached, stop send requests to sub-queue until sub-graph will be merged.
 * <p>
 * Read is instant: since it is just proof of concept we not provide transaction support,
 * read happens during write (read uncommitted).
 * One of a way how we can protect data from corruption during merge and other writes is:
 * - validate input data format (out of the box here during deserialization)
 * - create copy of subtree, which will bed changed, with keep in mind graph structure (can be circular references)
 * - can use, for example, Set for monitoring if graph node was copied or not
 * - if copied subtree changed successfully, switch original subtrees to changed one.
 * <p>
 * Exception handling policy (subject to change for prod version):
 * - throw exception before changing the graph
 * - during processing nodes: log exception with error level and ignore inconsistent changes
 *
 * @param <T> graph value type
 */
@Slf4j
public class LwwGraph<T> implements ILwwGraph<T> {
  public static final UUID ROOT_UID = UUID.fromString("0-0-0-0-0");

  @Getter
  private final AtomicLong version = new AtomicLong(System.nanoTime());
  private final AtomicBoolean isQueueFilled = new AtomicBoolean(false);
  private final Queue<LwwGraphOperation<T>> operationQueue = new ArrayDeque<>();
  @Getter
  private final LwwGraphNode<T> rootNode = new LwwGraphNode<>(ROOT_UID);
  @Getter
  private final Map<UUID, Long> removedNodeMap = new HashMap<>();
  @Getter
  private final Map<UUID, LwwGraphNode<T>> nodeMap = new HashMap<>();

  private final LwwGraphOperationHandler<T> operationHandler;

  public LwwGraph() {
    nodeMap.put(ROOT_UID, rootNode);
    operationHandler = new LwwGraphOperationHandler<>(nodeMap, removedNodeMap);
  }

  /**
   * Add new or update modified node to graph.
   * Node version derived from operation, since pointless to add old node.
   *
   * @param node node to add
   * @return operation queue result: added to queue or not
   */
  @Override
  public boolean addNode(final LwwGraphNode<T> node) {
    return addToQueue(new LwwGraphOperation<>(System.nanoTime(), version.get(),
      LwwGraphOperationEnum.ADD, node));
  }

  /**
   * Add node list or update node list to graph with taking in account removed nodes.
   * Node version used from node itself, (!) not from operation (!),
   * since this method can be used to merge concurrent trees.
   *
   * @param nodeList list of nodes
   * @param inputRemovedNodeMap map of removed nodes
   * @return operation queue result: added to queue or not
   */
  @Override
  public boolean mergeTree(final List<LwwGraphNode<T>> nodeList, final Map<UUID, Long> inputRemovedNodeMap) {
    return addToQueue(new LwwGraphOperation<>(System.nanoTime(), version.get(),
      LwwGraphOperationEnum.MERGE, nodeList, inputRemovedNodeMap));
  }

  /**
   * Remove node from graph and unlink it from parents and children
   * Node version derived from operation.
   *
   * @param nodeId node UID to remove
   * @return true if operation added to queue, false otherwise (have to try again)
   */
  @Override
  public boolean removeNode(final UUID nodeId) {
    return addToQueue(new LwwGraphOperation<>(System.nanoTime(), version.get(),
      LwwGraphOperationEnum.REMOVE, nodeMap.get(nodeId)));
  }

  @Override
  public boolean moveNode(final UUID destinationId, final LwwGraphNode<T> node) {
    throw new UnsupportedOperationException("Move operation is not supported in current implementation.\n" +
      "Use combination of add new node and remove old one.");
  }

  @Override
  public boolean isNodeExist(final UUID nodeID) {
    return nodeMap.containsKey(nodeID);
  }

  @Override
  public Set<LwwGraphNode<T>> getConnectedNodeSet(final UUID nodeId) {
    if (!isNodeExist(nodeId)) {
      return null;
    }

    LwwGraphNode<T> node = nodeMap.get(nodeId);
    Set<UUID> nodeIdSet = new HashSet<>(node.getChildSet());
    nodeIdSet.addAll(node.getParentSet());

    return nodeIdSet
      .stream()
      .map(nodeMap::get)
      .collect(Collectors.toSet());
  }

  /**
   * Simple search thru root node
   * TODO; use optimized algorithm like A*
   *
   * @param startNodeId node to start from
   * @param endNodeId   node finish to
   * @return null if path not found or List of node representing a path
   * from start node (inclusively) to end node (inclusively)
   */
  @Override
  public List<LwwGraphNode<T>> findPath(final UUID startNodeId, final UUID endNodeId) {
    if (!isNodeExist(startNodeId) || !isNodeExist(endNodeId)) {
      return null;
    }
    List<LwwGraphNode<T>> startNodePath = findRootPath(startNodeId);
    List<LwwGraphNode<T>> endNodePath = findRootPath(endNodeId);
    if (startNodePath == null || endNodePath == null) {
      return null;
    }

    return pathFromClosestAncestor(startNodePath, endNodePath);
  }

  private List<LwwGraphNode<T>> pathFromClosestAncestor(final List<LwwGraphNode<T>> startNodePath,
                                                        final List<LwwGraphNode<T>> endNodePath) {
    List<LwwGraphNode<T>> result = new ArrayList<>();
    Set<LwwGraphNode<T>> endNodeSet = new HashSet<>(endNodePath);

    int indexStart;
    int indexEnd;
    LwwGraphNode<T> node;
    List<LwwGraphNode<T>> subList;
    for (indexStart = 0; indexStart < startNodePath.size(); ++indexStart) {
      node = startNodePath.get(indexStart);
      if (!endNodeSet.contains(node)) {
        continue;
      }

      result.addAll(startNodePath.subList(0, indexStart + 1));

      indexEnd = endNodePath.indexOf(node);
      subList = endNodePath.subList(0, indexEnd);
      Collections.reverse(subList);
      result.addAll(subList);

      return result;
    }

    result.addAll(startNodePath);
    result.add(rootNode);
    Collections.reverse(endNodePath);
    result.addAll(endNodePath);

    return result;
  }

  /**
   * find path from current node to the root, assuming that parent exist
   *
   * @param nodeId for find path to the root
   * @return null if path not found or List of nodes representing a path
   * from search node (inclusively) to root node (exclusively)
   */
  private List<LwwGraphNode<T>> findRootPath(final UUID nodeId) {
    if (!isNodeExist(nodeId)) {
      return null;
    }

    List<LwwGraphNode<T>> result = new ArrayList<>();
    if (nodeId == ROOT_UID) {
      return result;
    }

    Map<LwwGraphNode<T>, Queue<UUID>> visitedNodeMap = new HashMap<>();
    Deque<LwwGraphNode<T>> visitedDeque = new ArrayDeque<>();
    Set<LwwGraphNode<T>> duplicateSet = new HashSet<>();
    UUID parentId = nodeId;

    LwwGraphNode<T> current;
    LwwGraphNode<T> next;
    while (parentId != ROOT_UID) {
      current = nodeMap.get(parentId);
      if (!visitedNodeMap.containsKey(current)) {
        visitedDeque.addLast(current);
        visitedNodeMap.put(current, new ArrayDeque<>(current.getParentSet()));
      } else {
        duplicateSet.add(current);
      }

      while ((parentId = visitedNodeMap.get(current).poll()) == null) {
        current = visitedDeque.pollLast();
        if (current == null) {
          return null;
        }
      }
      next = visitedDeque.peekLast();
      if (next == null || next.getUid() != current.getUid()) {
        visitedDeque.addLast(current);
      }
    }

    result.addAll(visitedDeque);
    cleanDuplicateRange(result, duplicateSet);

    return result;
  }

  private void cleanDuplicateRange(final List<LwwGraphNode<T>> result, final Set<LwwGraphNode<T>> duplicateSet) {
    int first;
    int last;
    for (LwwGraphNode<T> node : duplicateSet) {
      first = result.indexOf(node);
      last = result.lastIndexOf(node);
      if (first != last && first != -1 && last != -1) {
        result.subList(first, last).clear();
      }
    }
  }

  private boolean addToQueue(final LwwGraphOperation<T> operation) {
    /**
     *  Accordingly to doc, getAndUpdate should be side effect free
     */
    long newVersion = version.updateAndGet(lastValue -> {
      if (lastValue > operation.getPreviousVersion()) {
        return lastValue;
      }
      return operation.getVersion();
    });

    if (newVersion == operation.getVersion()) {
      log.debug("Adding operation to queue, {}", operation);
      operationQueue.add(operation);
      triggerQueue();
      return true;
    }

    return false;
  }

  private void triggerQueue() {
    if (isQueueFilled.compareAndSet(false, true)) {
      applyOperation();
    }
  }

  private void applyOperation() {
    LwwGraphOperation<T> operation;
    while ((operation = operationQueue.poll()) != null) {
      log.debug("Applying operation {}", operation);
      operationHandler.apply(operation);
    }
    if (isQueueFilled.compareAndSet(true, false)) {
      return;
    }

    /**
     * TODO; in case of deep recursion, rewrite to loop with queue solution
     */
    applyOperation();
  }
}
