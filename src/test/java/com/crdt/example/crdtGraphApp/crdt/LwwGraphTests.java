package com.crdt.example.crdtGraphApp.crdt;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class LwwGraphTests {
  private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors() - 1;
  private LwwGraph<Integer> concurrentGraph;
  private AtomicInteger lastThreadId = new AtomicInteger(0);

  @Test
  void createSimpleGraphTest() {
    LwwGraph<Integer> graph = new LwwGraph<>();
    LwwGraphNode<Integer> node = new LwwGraphNode<>(UUID.randomUUID(), 999, System.nanoTime());
    node.getParentSet().add(LwwGraph.ROOT_UID);
    graph.addNode(node);

    Assertions.assertTrue(graph.getRootNode().getChildSet().contains(node.getUid()));
    Assertions.assertTrue(node.getParentSet().contains(graph.getRootNode().getUid()));
    Assertions.assertEquals( 2, graph.getNodeMap().size());
  }

  @Test
  void modifySimpleGraphTest() {
    LwwGraph<Integer> graph = new LwwGraph<>();
    LwwGraphNode<Integer> node = new LwwGraphNode<>(UUID.randomUUID(), 999, System.nanoTime());
    node.getParentSet().add(LwwGraph.ROOT_UID);
    graph.addNode(node);

    LwwGraphNode<Integer> nodeCopy = node.copy();
    nodeCopy.setValue(777);
    graph.addNode(nodeCopy);

    Assertions.assertEquals(nodeCopy.getValue(), graph.getNodeMap().get(nodeCopy.getUid()).getValue());
  }

  @Test
  void createGraphWithMergeTest() {
    LwwGraph<Integer> graph = new LwwGraph<>();
    List<LwwGraphNode<Integer>> nodeList = createNodeList();

    graph.mergeTree(nodeList, new HashMap<>());
    Assertions.assertEquals(101, graph.getNodeMap().size());
    Assertions.assertEquals(0, graph.getRemovedNodeMap().size());
  }

  @Test
  void createGraphWithMergeAndRemovedMapTest() {
    LwwGraph<Integer> graph = new LwwGraph<>();
    List<LwwGraphNode<Integer>> nodeList = createNodeList();

    // test if graph removedMap is not empty
    graph.mergeTree(nodeList, new HashMap<>());
    graph.removeNode(nodeList.get(84).getUid());
    graph.removeNode(nodeList.get(72).getUid());

    Assertions.assertEquals(99, graph.getNodeMap().size());
    Assertions.assertEquals(2, graph.getRemovedNodeMap().size());

    // test if input removedMap merged with graph removedMap
    Map<UUID, Long> removedMap = new HashMap<>();
    removedMap.put(nodeList.get(50).getUid(), System.nanoTime());
    removedMap.put(nodeList.get(33).getUid(), System.nanoTime());

    graph.mergeTree(new ArrayList<>(), removedMap);

    Assertions.assertEquals(4, graph.getRemovedNodeMap().size());
    Assertions.assertEquals(97, graph.getNodeMap().size());

    // test if input removedMap merged with graph removedMap and nodeList merged 2nd times
    removedMap.put(nodeList.get(1).getUid(), System.nanoTime());
    removedMap.put(nodeList.get(2).getUid(), System.nanoTime());

    graph.mergeTree(nodeList, removedMap);

    Assertions.assertEquals(6, graph.getRemovedNodeMap().size());
    Assertions.assertEquals(95, graph.getNodeMap().size());

    // test deleted nodes not exist in graph
    Assertions.assertFalse(graph.isNodeExist(nodeList.get(1).getUid()));
    Assertions.assertFalse(graph.isNodeExist(nodeList.get(2).getUid()));
    Assertions.assertFalse(graph.isNodeExist(nodeList.get(33).getUid()));
    Assertions.assertFalse(graph.isNodeExist(nodeList.get(84).getUid()));
  }

  @Test
  void findPathTest() {
    LwwGraph<Integer> graph = new LwwGraph<>();
    List<LwwGraphNode<Integer>> nodeList = createNodeList();
    graph.mergeTree(nodeList, new HashMap<>());

    nodeList.get(10).getParentSet().add(LwwGraph.ROOT_UID);
    nodeList.get(15).getParentSet().add(nodeList.get(10).getUid());
    nodeList.get(13).getParentSet().add(nodeList.get(15).getUid());

    nodeList.get(96).getParentSet().add(LwwGraph.ROOT_UID);
    nodeList.get(92).getParentSet().add(nodeList.get(96).getUid());
    nodeList.get(90).getParentSet().add(nodeList.get(92).getUid());

    log.debug("13 {}", nodeList.get(13));
    log.debug("90 {}", nodeList.get(90));

    List<LwwGraphNode<Integer>> result = graph.findPath(nodeList.get(13).getUid(), nodeList.get(90).getUid());
    Assertions.assertNotNull(result);

    List<UUID> idList = result.stream()
      .map(LwwGraphNode::getUid)
      .collect(Collectors.toList());
    log.debug("idList {}", idList);

    Assertions.assertEquals(nodeList.get(13), result.get(0));
    Assertions.assertEquals(nodeList.get(90), result.get(result.size() - 1));
  }

  @Test
  void concurrentMergeTest() throws InterruptedException, ExecutionException, IllegalArgumentException {
    concurrentGraph = new LwwGraph<>();
    ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
    ArrayList<Callable<Boolean>> tasks = new ArrayList<>();

    int index;
    for (index = 0; index < THREAD_COUNT; ++index) {
      tasks.add(this::generateTask);
    }

    List<Future<Boolean>> taskResults = executor.invokeAll(tasks);
    boolean resultStatus = true;
    for (Future<Boolean> result : taskResults) {
      resultStatus = result.get() && resultStatus;
    }
    executor.shutdown();

    if (!resultStatus) {
      log.info("FINISHED with error, taskResults: {}", taskResults);
    } else {
      log.info("SUCCESS");
    }

    // at least half successful nodes added from first operation try
    // TODO: use repeat statement to achieve 100%
    Assertions.assertTrue(concurrentGraph.getNodeMap().size() > THREAD_COUNT/2*100);
    log.info("concurrentGraph node count: expected {}, actual {}", THREAD_COUNT*100+1, concurrentGraph.getNodeMap().size());

    LwwGraphNode<Integer> node = getSecondNode();
    Assertions.assertEquals(lastThreadId.get(), node.getValue());
  }

  private boolean generateTask() {
    List<LwwGraphNode<Integer>> nodeList = createNodeList();
    concurrentGraph.mergeTree(nodeList, new HashMap<>());

    try {
      Thread.sleep(1000);


      int threadId = (int) Thread.currentThread().getId();
      LwwGraphNode<Integer> node = getSecondNode().copy();
      node.setValue(threadId);
      concurrentGraph.addNode(node);
      log.debug("*****************\nModified node {} from thread {} {}", node, Thread.currentThread().getName(), threadId);

      lastThreadId.set(threadId);
      return true;
    } catch (InterruptedException ex) {
      log.error("Generate Thread has been interrupted already", ex);
      log.error("Generate Thread: {}", Thread.currentThread());
      return false;
    }
  }

  private LwwGraphNode<Integer> getSecondNode() {
    Iterator<Map.Entry<UUID, LwwGraphNode<Integer>>> iterator = concurrentGraph.getNodeMap().entrySet().iterator();
    iterator.next();
    return iterator.next().getValue();
  }

  private List<LwwGraphNode<Integer>> createNodeList() {
    List<LwwGraphNode<Integer>> nodeList = new ArrayList<>();

    for (int index = 0; index < 100; ++index) {
      nodeList.add(new LwwGraphNode<>(UUID.randomUUID(), index));
    }
    nodeList.get(0).getParentSet().add(LwwGraph.ROOT_UID);

    Random random = new Random();
    for (int index = 1; index < 100; ++index) {
      for (int parentIndex = 0; parentIndex < 5; ++parentIndex) {
        nodeList.get(index).getParentSet().add(
          nodeList.get(random.nextInt(100)).getUid()
        );
      }
    }

    return nodeList;
  }
}
