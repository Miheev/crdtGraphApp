package com.crdt.example.crdtGraphApp.crdt.operation;

import com.crdt.example.crdtGraphApp.crdt.LwwGraphNode;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Data
@RequiredArgsConstructor
public final class LwwGraphOperation<T> {
  private final long version;
  private final long previousVersion;
  private final LwwGraphOperationEnum operationType;

  private LwwGraphNode<T> dataNode;
  private List<LwwGraphNode<T>> dataNodeList;
  private Map<UUID, Long> removedNodeMap;

  public LwwGraphOperation(final long version, final long previousVersion, final LwwGraphOperationEnum operationType,
                           final LwwGraphNode<T> dataNode) {
    this(version, previousVersion, operationType);
    setDataNode(dataNode);
  }

  public LwwGraphOperation(final long version, final long previousVersion, final LwwGraphOperationEnum operationType,
                           final List<LwwGraphNode<T>> dataNodeList, final Map<UUID, Long> removedNodeMap) {
    this(version, previousVersion, operationType);
    setDataNodeList(dataNodeList);
    setRemovedNodeMap(removedNodeMap);
  }

  public boolean isOperationList() {
    return dataNodeList != null && removedNodeMap != null;
  }
}
