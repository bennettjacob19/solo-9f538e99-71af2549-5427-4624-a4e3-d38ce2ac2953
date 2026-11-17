"""FastAPI 历史查询网页：源目录成组修改、提交点查询、捕获进度。

初始化未完成（phase != STREAMING）时，所有历史查询接口返回 503，
不对外开放尚未完整的历史；源目录修改走源库自身事务，不受此限制。
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Annotated, Literal, Union

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field, field_validator

from .capture import CaptureWorker
from .config import settings
from .source import SourceDB, SourceError
from .storage import (
    HistoryStore,
    NotStreamingError,
    PointNotCapturedError,
    PointRetainedAwayError,
    lsn_to_num,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("web")

SHA256_RE = r"^[0-9a-fA-F]{64}$"


def _check_sha(v: str) -> str:
    import re

    if not re.match(SHA256_RE, v):
        raise ValueError("sha256 必须是 64 位十六进制")
    return v


class _Columns(BaseModel):
    """增/改共用的四列固定字段，严格拒绝额外列。"""

    artifact_no: int = Field(gt=0)
    service_name: str = Field(min_length=1, max_length=200)
    version: str = Field(min_length=1, max_length=100)
    sha256: str

    @field_validator("sha256")
    @classmethod
    def _sha(cls, v: str) -> str:
        return _check_sha(v)


class InsertOp(_Columns):
    op: Literal["insert"]


class UpdateOp(_Columns):
    op: Literal["update"]


class DeleteOp(BaseModel):
    op: Literal["delete"]
    artifact_no: int = Field(gt=0)


AnyOp = Annotated[
    Union[InsertOp, UpdateOp, DeleteOp], Field(discriminator="op")
]


class BatchRequest(BaseModel):
    operations: list[AnyOp] = Field(min_length=1)


class RetainRequest(BaseModel):
    """网页选定一个已捕获提交点，将该点目录固化为新基线。"""

    point: str = Field(min_length=1, max_length=32)
    crash_before_commit: bool = False  # 仅测试钩子使用，默认关闭


store = HistoryStore(settings.sqlite_path)
source = SourceDB(settings)
worker = CaptureWorker(store, source, settings)


@asynccontextmanager
async def lifespan(app: FastAPI):
    worker.start()
    try:
        yield
    finally:
        worker.stop()
        store.close()


app = FastAPI(title="制品目录历史查询", lifespan=lifespan)


def _require_ready() -> None:
    if not store.is_ready():
        progress = store.get_progress()
        raise HTTPException(
            status_code=503,
            detail={
                "message": "捕获初始化尚未完成，历史暂不开放",
                "phase": progress.get("phase"),
                "last_error": progress.get("last_error"),
            },
        )


# --------------------------------------------------------------------- 页面
@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(
        os.path.join(os.path.dirname(__file__), "static", "index.html")
    )


# --------------------------------------------------------------------- 进度
@app.get("/api/progress")
def api_progress() -> dict:
    return store.get_progress()


# --------------------------------------------------------------- 源目录读写
@app.get("/api/source/artifacts")
def api_source_list() -> dict:
    return {"artifacts": source.list_artifacts()}


@app.post("/api/source/operations")
def api_batch_write(req: BatchRequest) -> dict:
    """成组修改源目录；整组在一个源事务内提交，失败整体回滚。"""
    payload = [op.model_dump() for op in req.operations]
    try:
        result = source.apply_operations(payload)
    except SourceError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"源库错误: {exc}")
    result["operations"] = len(payload)
    return result


# ---------------------------------------------------------------- 历史查询
@app.get("/api/points")
def api_points() -> dict:
    _require_ready()
    return {"points": store.get_points()}


@app.get("/api/directory")
def api_directory(point: str) -> JSONResponse:
    """按提交点（LSN，如 0/16B3748）查看该时刻的整张目录。"""
    _require_ready()
    try:
        lsn_num = lsn_to_num(point)
    except (ValueError, IndexError):
        raise HTTPException(status_code=400, detail="point 必须是 LSN，形如 0/16B3748")
    status = store.is_point_queryable(lsn_num)
    if status == "gone":
        # 旧提交点已被基线固化清理：明确告知不可查，而不是返回错误数据
        raise HTTPException(
            status_code=410,
            detail={
                "message": "该提交点早于保留起点，逐笔历史已被基线固化清理，不可查询",
                "point": point,
                "retention_horizon_lsn": store.get_progress().get(
                    "retention_horizon_lsn"
                ),
            },
        )
    if status == "unknown":
        raise HTTPException(
            status_code=404,
            detail="该 LSN 不是已捕获的提交点，请先在 /api/points 中选择",
        )
    return JSONResponse(
        {
            "point": point,
            "point_lsn_num": lsn_num,
            "artifacts": store.get_directory_at(lsn_num),
        }
    )


@app.get("/api/history/{artifact_no}")
def api_artifact_history(artifact_no: int) -> dict:
    """单个制品编号自当前基线起的完整变更链；删除后重用编号，旧历史也保留。"""
    _require_ready()
    return {
        "artifact_no": artifact_no,
        "history": store.get_history_of(artifact_no),
    }


# ------------------------------------------------------------ 基线固化/清理
@app.post("/api/retention/freeze")
def api_freeze(req: RetainRequest) -> dict:
    """把选定提交点的整张目录固化为新基线，清除更早的逐笔变更。"""
    _require_ready()
    try:
        lsn_to_num(req.point)
    except (ValueError, IndexError):
        raise HTTPException(status_code=400, detail="point 必须是 LSN，形如 0/16B3748")
    try:
        # 崩溃钩子仅在显式环境变量开启时生效，生产环境强制忽略该字段
        crash = req.crash_before_commit and settings.enable_freeze_crash_hook
        result = store.freeze_baseline(
            req.point, crash_before_commit=crash
        )
    except PointRetainedAwayError as exc:
        raise HTTPException(status_code=410, detail=str(exc))
    except PointNotCapturedError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except NotStreamingError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    log.info("baseline frozen at %s: %s", req.point, result)
    return {"message": "基线固化完成", **result}


@app.get("/api/retention/events")
def api_retention_events() -> dict:
    """历次基线固化（历史清理）审计记录。"""
    _require_ready()
    return {"events": store.get_retention_events()}
