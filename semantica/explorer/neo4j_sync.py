"""Explorer ↔ Neo4j 同步层（穿山甲项目统一图基座）。

设计要点
--------
- Neo4j 是**唯一真相源**（unified graph store）；Explorer 内存 ``ContextGraph``
  负责交互/分析加速。
- 启动时 ``load_graph_from_neo4j`` 全量加载；每次图变更经 ``sync_mutation``
  增量落库；``reload_graph`` 供 ``/api/graph/reload`` 重新全量加载。
- 节点统一打 ``KGNode`` 标签，业务主键存为 ``kgNodeId`` 属性（避免与 Neo4j
  内部 ``id()`` 混淆）；边用 ``kgEdgeId`` 属性做主键。
- ``ContextGraph`` 的 ``mutation_callback`` payload 直接就是 ``node.to_dict()``
  / ``edge.to_dict()``，故同步层无需再查图，直接映射即可。
- 非标量属性（dict/list）json 序列化后兜底存储，保证不丢结构且可回放。

环境变量（由 docker-compose 注入）
----------------------------------
- ``GRAPH_STORE_NEO4J_URI``      未设置则整个同步层不启用
- ``GRAPH_STORE_NEO4J_USER``     默认 neo4j
- ``GRAPH_STORE_NEO4J_PASSWORD`` 默认 password
- ``GRAPH_STORE_NEO4J_DATABASE`` 默认 neo4j
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

from ..context.context_graph import ContextGraph
from ..graph_store.neo4j_store import Neo4jStore

logger = logging.getLogger(__name__)

KG_NODE_LABEL = "KGNode"
NODE_PK = "kgNodeId"
EDGE_PK = "kgEdgeId"


def make_store() -> Optional[Neo4jStore]:
    """从环境变量构建 Neo4jStore（lazy connect）。未配置 URI 时返回 None。"""
    uri = os.environ.get("GRAPH_STORE_NEO4J_URI")
    if not uri:
        logger.info("GRAPH_STORE_NEO4J_URI 未设置，跳过 Neo4j 同步。")
        return None
    user = os.environ.get("GRAPH_STORE_NEO4J_USER", "neo4j")
    password = os.environ.get("GRAPH_STORE_NEO4J_PASSWORD", "password")
    database = os.environ.get("GRAPH_STORE_NEO4J_DATABASE", "neo4j")
    try:
        return Neo4jStore(uri=uri, user=user, password=password, database=database)
    except Exception as exc:  # noqa: BLE001
        logger.warning("创建 Neo4jStore 失败：%s", exc)
        return None


# --------------------------------------------------------------------------- #
# 属性序列化：标量直接存，非标量 json 兜底，保证 Neo4j 友好且可回放
# --------------------------------------------------------------------------- #
def _flatten_props(props: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    flat: Dict[str, Any] = {}
    for key, value in (props or {}).items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            flat[key] = value
        else:
            flat[key] = json.dumps(value, ensure_ascii=False, default=str)
    return flat


def _unflatten_props(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, value in (raw or {}).items():
        if isinstance(value, str):
            try:
                out[key] = json.loads(value)
                continue
            except (json.JSONDecodeError, TypeError):
                pass
        out[key] = value
    return out


def _safe_rel(rel_type: Optional[str]) -> str:
    """Neo4j 关系类型必须是不含空格的标识符；非法字符统一替换为下划线。"""
    safe = "".join(
        c if c.isalnum() or c == "_" else "_" for c in (rel_type or "related_to")
    )
    return safe or "RELATED_TO"


# 已知词表前缀反向映射：Neo4j 关系类型里的 ``_`` 还原回 ``:``。
# 仅用于读回历史数据时兜底（新写入已通过 r.relType 存原始类型）。
_RESTORE_REL_TYPES = {
    "rdfs_subClassOf": "rdfs:subClassOf",
    "rdfs_subPropertyOf": "rdfs:subPropertyOf",
    "rdfs_domain": "rdfs:domain",
    "rdfs_range": "rdfs:range",
    "owl_sameAs": "owl:sameAs",
    "owl_inverseOf": "owl:inverseOf",
    "owl_equivalentClass": "owl:equivalentClass",
    "sh_path": "sh:path",
    "skos_broader": "skos:broader",
    "skos_narrower": "skos:narrower",
    "skos_related": "skos:related",
}


def _restore_rel_type(raw_type: Optional[str], rel_type_prop: Optional[str]) -> str:
    """读回边类型：优先用写入时保存的原始类型，否则对历史数据做反向映射。"""
    if rel_type_prop:
        return rel_type_prop
    if raw_type and raw_type in _RESTORE_REL_TYPES:
        return _RESTORE_REL_TYPES[raw_type]
    return raw_type or "related_to"


# --------------------------------------------------------------------------- #
# 读取 / 加载
# --------------------------------------------------------------------------- #
def _read_graph_dict(store: Neo4jStore) -> Dict[str, Any]:
    """从 Neo4j 读全量图，返回 ContextGraph.from_dict 接受的 {nodes, edges}。"""
    node_res = store.execute_query(
        f"MATCH (n:{KG_NODE_LABEL}) "
        f"RETURN n.{NODE_PK} AS id, n.nodeType AS type, n.content AS content, "
        f"n.propsJson AS propsJson, n.scheme_uri AS scheme_uri, "
        f"n.ontologyUri AS ontologyUri, n.draftState AS draftState"
    )
    nodes: List[Dict[str, Any]] = []
    for rec in (node_res.get("records") or []):
        raw = rec.get("propsJson")
        props = _unflatten_props(json.loads(raw)) if raw else {}
        # Re-inject ontology bookkeeping that was written as plain Neo4j
        # properties (not inside propsJson) so in-memory graph nodes carry
        # scheme_uri / ontologyUri / draftState for Registry and Editor.
        if rec.get("scheme_uri") or rec.get("ontologyUri"):
            props.setdefault("scheme_uri", rec.get("scheme_uri") or rec.get("ontologyUri"))
        if rec.get("ontologyUri"):
            props.setdefault("ontologyUri", rec["ontologyUri"])
        if rec.get("draftState"):
            props.setdefault("draftState", rec["draftState"])
        nodes.append(
            {
                "id": rec["id"],
                "type": rec.get("type") or "entity",
                # ContextNode.to_dict 的 content 在 properties 里，优先取它。
                "content": props.get("content") or rec.get("content") or rec["id"],
                "properties": props,
            }
        )

    edge_res = store.execute_query(
        f"MATCH (a:{KG_NODE_LABEL})-[r]->(b:{KG_NODE_LABEL}) "
        f"RETURN a.{NODE_PK} AS source, b.{NODE_PK} AS target, "
        f"r.{EDGE_PK} AS eid, type(r) AS type, r.relType AS relType, "
        f"r.propsJson AS propsJson, r.weight AS weight"
    )
    edges: List[Dict[str, Any]] = []
    for rec in (edge_res.get("records") or []):
        raw = rec.get("propsJson")
        props = _unflatten_props(json.loads(raw)) if raw else {}
        edges.append(
            {
                "id": rec.get("eid"),
                "source": rec["source"],
                "target": rec["target"],
                "type": _restore_rel_type(rec.get("type"), rec.get("relType")),
                "weight": rec.get("weight", 1.0),
                "properties": props,
            }
        )
    return {"nodes": nodes, "edges": edges}


def load_graph_from_neo4j(store: Neo4jStore) -> ContextGraph:
    """从 Neo4j 全量加载到一个新的（轻量）ContextGraph。"""
    graph = ContextGraph(advanced_analytics=False)
    graph_dict = _read_graph_dict(store)
    graph.from_dict(graph_dict)
    logger.info(
        "从 Neo4j 加载图：%d 节点 / %d 边",
        len(graph_dict["nodes"]),
        len(graph_dict["edges"]),
    )
    return graph


def reload_graph(store: Neo4jStore, session: Any) -> None:
    """用 Neo4j 当前数据刷新运行中的 session 内存图。

    直接对现有 ``session.graph`` 调 ``from_dict``（走内部 add，不触发
    mutation_callback），保留已挂载的 mutation bridge 与回调。
    """
    graph_dict = _read_graph_dict(store)
    session.graph._suspend_mutation_callback = True
    try:
        session.graph.from_dict(graph_dict)
    finally:
        session.graph._suspend_mutation_callback = False
    session.rebuild_search_index()
    logger.info(
        "Explorer 内存图已从 Neo4j 重新加载：%d 节点 / %d 边",
        len(graph_dict["nodes"]),
        len(graph_dict["edges"]),
    )


# --------------------------------------------------------------------------- #
# 增量同步（mutation -> Neo4j）
# --------------------------------------------------------------------------- #
def sync_mutation(
    store: Neo4jStore,
    event_type: str,
    entity_id: str,
    payload: Dict[str, Any],
) -> None:
    """把单条 mutation 增量同步到 Neo4j。失败仅告警，不影响主流程。"""
    try:
        et = (event_type or "").upper()
        if et in ("ADD_NODE", "UPDATE_NODE"):
            _upsert_node(store, payload)
        elif et in ("ADD_EDGE", "UPDATE_EDGE"):
            _upsert_edge(store, payload)
        elif et == "REMOVE_NODE":
            store.execute_query(
                f"MATCH (n:{KG_NODE_LABEL} {{{NODE_PK}: $pk}}) DETACH DELETE n",
                {"pk": entity_id},
            )
        elif et == "REMOVE_EDGE":
            store.execute_query(
                f"MATCH ()-[r]-() WHERE r.{EDGE_PK} = $pk DELETE r",
                {"pk": entity_id},
            )
        else:
            logger.debug("忽略未处理的 mutation 类型：%s", event_type)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Neo4j 同步失败（%s %s）：%s", event_type, entity_id, exc)


def _upsert_node(store: Neo4jStore, payload: Dict[str, Any]) -> None:
    node_id = payload.get("id")
    if not node_id:
        return
    raw_props = dict(payload.get("properties") or {})
    # ContextNode.to_dict 把 content 放在 properties 里而非顶层。
    node_type = payload.get("type") or raw_props.get("type") or "entity"
    content = payload.get("content") or raw_props.get("content") or node_id
    props: Dict[str, Any] = raw_props
    meta = payload.get("metadata")
    if isinstance(meta, dict):
        props.update(meta)
    flat = _flatten_props(props)
    store.execute_query(
        f"MERGE (n:{KG_NODE_LABEL} {{{NODE_PK}: $pk}}) "
        f"SET n.nodeType = $nodeType, n.content = $content, n.propsJson = $propsJson "
        f"SET n += $flat",
        {
            "pk": node_id,
            "nodeType": node_type,
            "content": content,
            "propsJson": json.dumps(props, ensure_ascii=False, default=str),
            "flat": flat,
        },
    )


def _upsert_edge(store: Neo4jStore, payload: Dict[str, Any]) -> None:
    # ContextEdge.to_dict 的字段名是 source_id / target_id（非 source / target）。
    source = payload.get("source_id") or payload.get("source")
    target = payload.get("target_id") or payload.get("target")
    edge_id = payload.get("id")
    edge_type = payload.get("type", "related_to")
    if not (source and target):
        return
    props: Dict[str, Any] = dict(payload.get("properties") or {})
    meta = payload.get("metadata")
    if isinstance(meta, dict):
        props.update(meta)
    if "weight" in payload and payload["weight"] is not None:
        props["weight"] = payload["weight"]
    store.execute_query(
        f"MATCH (a:{KG_NODE_LABEL} {{{NODE_PK}: $src}}), "
        f"(b:{KG_NODE_LABEL} {{{NODE_PK}: $tgt}}) "
        f"MERGE (a)-[r:{_safe_rel(edge_type)} {{{EDGE_PK}: $eid}}]->(b) "
        f"SET r.propsJson = $propsJson, r.weight = $weight, r.relType = $relType "
        f"SET r += $flat",
        {
            "src": source,
            "tgt": target,
            "eid": edge_id,
            "propsJson": json.dumps(props, ensure_ascii=False, default=str),
            "weight": float(payload.get("weight", 1.0)),
            "relType": edge_type,
            "flat": _flatten_props(props),
        },
    )


# --------------------------------------------------------------------------- #
# Ontology 持久化（Phase 2：本体落库，draft 也同步）
# --------------------------------------------------------------------------- #
ONTO_DRAFT_LABEL = "OntologyDraft"
ONTO_PROPOSAL_LABEL = "OntologyProposal"
ONTO_VERSION_LABEL = "OntologyVersion"


def _entity_label(uri: str) -> str:
    """从 URI 提取短标签（取最后一段的 # 或 / 之后部分）。"""
    return (uri.rsplit("/", 1)[-1].rsplit("#", 1)[-1]) or uri


def sync_ontology_draft(store: Neo4jStore, draft: Dict[str, Any]) -> None:
    """把 draft 文档及其涉及的类/属性 URI 落 Neo4j。

    - draft 文档本身存为 ``OntologyDraft`` 节点（含完整 diffJson）。
    - 新增类/属性 MERGE 为 ``KGNode``，标记 ``draftState='draft'``。
    - 删除类/属性标记 ``draftState='draft_removed'``（不真删，待 publish 时处理）。
    """
    try:
        draft_id = draft.get("draft_id", "")
        ontology_uri = draft.get("ontology_uri", "")
        diff = draft.get("diff") or {}
        diff_json = json.dumps(diff, ensure_ascii=False, default=str)

        store.execute_query(
            f"MERGE (d:{ONTO_DRAFT_LABEL} {{draftId: $did}}) "
            f"SET d.ontologyUri = $ouri, d.author = $author, "
            f"d.summary = $summary, d.createdAt = $ca, d.updatedAt = $ua, "
            f"d.diffJson = $dj",
            {
                "did": draft_id,
                "ouri": ontology_uri,
                "author": draft.get("author", ""),
                "summary": draft.get("summary") or "",
                "ca": draft.get("created_at", ""),
                "ua": draft.get("updated_at", ""),
                "dj": diff_json,
            },
        )

        for uri in diff.get("added_classes", []):
            _upsert_draft_entity(store, uri, "owl:Class", draft_id, ontology_uri)
        for uri in diff.get("added_properties", []):
            _upsert_draft_entity(store, uri, "owl:ObjectProperty", draft_id, ontology_uri)

        # 处理 label / 属性修改：把用户自定义的 label 写回 KGNode.content
        modified_classes = diff.get("modified_classes") or {}
        modified_properties = diff.get("modified_properties") or {}
        for uri, changes in {**modified_classes, **modified_properties}.items():
            if not isinstance(changes, dict):
                continue
            custom_label = changes.get("label")
            if custom_label is None:
                continue
            store.execute_query(
                f"MATCH (n:{KG_NODE_LABEL} {{{NODE_PK}: $uri}}) "
                f"SET n.content = $label",
                {"uri": uri, "label": str(custom_label)},
            )

        for uri in diff.get("removed_classes", []) + diff.get("removed_properties", []):
            store.execute_query(
                f"MATCH (n:{KG_NODE_LABEL} {{{NODE_PK}: $uri}}) "
                f"SET n.draftState = 'draft_removed', n.draftId = $did",
                {"uri": uri, "did": draft_id},
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Ontology draft 同步失败（%s）：%s", draft.get("draft_id"), exc)


def _upsert_draft_entity(
    store: Neo4jStore, uri: str, node_type: str, draft_id: str, ontology_uri: str
) -> None:
    label = _entity_label(uri)
    store.execute_query(
        f"MERGE (n:{KG_NODE_LABEL} {{{NODE_PK}: $uri}}) "
        f"SET n.nodeType = $nt, n.content = $label, "
        f"n.draftState = 'draft', n.draftId = $did, n.ontologyUri = $ouri, "
        f"n.scheme_uri = $ouri",
        {
            "uri": uri,
            "nt": node_type,
            "label": label,
            "did": draft_id,
            "ouri": ontology_uri,
        },
    )


def sync_ontology_proposal(store: Neo4jStore, proposal: Dict[str, Any]) -> None:
    """持久化 proposal（含 state 变更：proposed/approved/rejected/published）。"""
    try:
        store.execute_query(
            f"MERGE (p:{ONTO_PROPOSAL_LABEL} {{proposalId: $pid}}) "
            f"SET p.draftId = $did, p.ontologyUri = $ouri, p.author = $author, "
            f"p.state = $state, p.summary = $summary, "
            f"p.createdAt = $ca, p.updatedAt = $ua, "
            f"p.reviewer = $reviewer, p.impactJson = $ij, p.shaclJson = $sj, "
            f"p.commentsJson = $cj",
            {
                "pid": proposal.get("proposal_id", ""),
                "did": proposal.get("draft_id", ""),
                "ouri": proposal.get("ontology_uri", ""),
                "author": proposal.get("author", ""),
                "state": proposal.get("state", "proposed"),
                "summary": proposal.get("summary") or "",
                "ca": proposal.get("created_at", ""),
                "ua": proposal.get("updated_at", ""),
                "reviewer": proposal.get("reviewer") or "",
                "ij": json.dumps(
                    proposal.get("impact_analysis") or {}, ensure_ascii=False, default=str
                ),
                "sj": json.dumps(
                    proposal.get("shacl_validation") or {}, ensure_ascii=False, default=str
                ),
                "cj": json.dumps(
                    proposal.get("comments") or [], ensure_ascii=False, default=str
                ),
            },
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Ontology proposal 同步失败（%s）：%s", proposal.get("proposal_id"), exc)


def sync_ontology_publish(
    store: Neo4jStore,
    draft: Dict[str, Any],
    proposal: Dict[str, Any],
    version: str,
) -> None:
    """发布时：把 draft 实体标记为 published，持久化版本记录。"""
    try:
        diff = draft.get("diff") or {}
        ontology_uri = proposal.get("ontology_uri", "")
        for uri in diff.get("added_classes", []) + diff.get("added_properties", []):
            store.execute_query(
                f"MATCH (n:{KG_NODE_LABEL} {{{NODE_PK}: $uri}}) "
                f"SET n.draftState = 'published', n.version = $ver, n.ontologyUri = $ouri",
                {"uri": uri, "ver": version, "ouri": ontology_uri},
            )

        # publish 时若用户修改了已存在实体的 label，也要落盘
        modified_classes = diff.get("modified_classes") or {}
        modified_properties = diff.get("modified_properties") or {}
        for uri, changes in {**modified_classes, **modified_properties}.items():
            if not isinstance(changes, dict):
                continue
            custom_label = changes.get("label")
            if custom_label is None:
                continue
            store.execute_query(
                f"MATCH (n:{KG_NODE_LABEL} {{{NODE_PK}: $uri}}) "
                f"SET n.content = $label, n.draftState = 'published', n.version = $ver, n.ontologyUri = $ouri",
                {"uri": uri, "label": str(custom_label), "ver": version, "ouri": ontology_uri},
            )
        store.execute_query(
            f"MERGE (v:{ONTO_VERSION_LABEL} {{versionId: $vid}}) "
            f"SET v.ontologyUri = $ouri, v.author = $author, "
            f"v.date = $date, v.state = 'published', "
            f"v.diffSummaryJson = $dj",
            {
                "vid": version,
                "ouri": proposal.get("ontology_uri", ""),
                "author": proposal.get("author", ""),
                "date": proposal.get("updated_at", ""),
                "dj": json.dumps(diff, ensure_ascii=False, default=str),
            },
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Ontology publish 同步失败：%s", exc)


def load_ontology_state(store: Neo4jStore) -> Dict[str, Any]:
    """从 Neo4j 加载 drafts / proposals / versions（供 app.state 启动初始化）。

    返回格式与 Pydantic 模型构造函数的入参一致，调用方负责 ``Model(**dict)``。
    """
    result: Dict[str, Any] = {"drafts": [], "proposals": [], "versions": {}}

    try:
        dres = store.execute_query(
            f"MATCH (d:{ONTO_DRAFT_LABEL}) "
            f"RETURN d.draftId AS draftId, d.ontologyUri AS ontologyUri, "
            f"d.author AS author, d.summary AS summary, "
            f"d.createdAt AS createdAt, d.updatedAt AS updatedAt, "
            f"d.diffJson AS diffJson"
        )
        for rec in (dres.get("records") or []):
            raw_diff = rec.get("diffJson")
            diff = json.loads(raw_diff) if raw_diff else {}
            result["drafts"].append(
                {
                    "draft_id": rec.get("draftId"),
                    "ontology_uri": rec.get("ontologyUri", ""),
                    "diff": diff,
                    "author": rec.get("author", ""),
                    "summary": rec.get("summary") or None,
                    "created_at": rec.get("createdAt", ""),
                    "updated_at": rec.get("updatedAt", ""),
                }
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("加载 OntologyDraft 失败：%s", exc)

    try:
        pres = store.execute_query(
            f"MATCH (p:{ONTO_PROPOSAL_LABEL}) "
            f"RETURN p.proposalId AS proposalId, p.draftId AS draftId, "
            f"p.ontologyUri AS ontologyUri, p.author AS author, "
            f"p.state AS state, p.summary AS summary, "
            f"p.createdAt AS createdAt, p.updatedAt AS updatedAt, "
            f"p.reviewer AS reviewer, p.impactJson AS impactJson, "
            f"p.shaclJson AS shaclJson, p.commentsJson AS commentsJson"
        )
        for rec in (pres.get("records") or []):
            raw_ij = rec.get("impactJson")
            raw_sj = rec.get("shaclJson")
            raw_cj = rec.get("commentsJson")
            result["proposals"].append(
                {
                    "proposal_id": rec.get("proposalId"),
                    "draft_id": rec.get("draftId", ""),
                    "ontology_uri": rec.get("ontologyUri", ""),
                    "summary": rec.get("summary") or "",
                    "author": rec.get("author", ""),
                    "reviewer": rec.get("reviewer") or None,
                    "state": rec.get("state", "proposed"),
                    "impact_analysis": json.loads(raw_ij) if raw_ij else {},
                    "shacl_validation": json.loads(raw_sj) if raw_sj else {},
                    "created_at": rec.get("createdAt", ""),
                    "updated_at": rec.get("updatedAt", ""),
                    "comments": json.loads(raw_cj) if raw_cj else [],
                }
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("加载 OntologyProposal 失败：%s", exc)

    try:
        vres = store.execute_query(
            f"MATCH (v:{ONTO_VERSION_LABEL}) "
            f"RETURN v.versionId AS versionId, v.ontologyUri AS ontologyUri, "
            f"v.author AS author, v.date AS date, v.state AS state, "
            f"v.diffSummaryJson AS diffSummaryJson"
        )
        for rec in (vres.get("records") or []):
            ouri = rec.get("ontologyUri", "")
            raw_ds = rec.get("diffSummaryJson")
            result["versions"].setdefault(ouri, []).append(
                {
                    "version_id": rec.get("versionId"),
                    "ontology_uri": ouri,
                    "state": "published",
                    "author": rec.get("author", ""),
                    "date": rec.get("date", ""),
                    "diff_summary": json.loads(raw_ds) if raw_ds else {},
                }
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("加载 OntologyVersion 失败：%s", exc)

    return result
