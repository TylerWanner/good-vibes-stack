from __future__ import annotations

import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime as _datetime
from typing import Any, Annotated

from fastapi import FastAPI, HTTPException, Query, UploadFile, File, Form, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field
import uvicorn

try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.util import get_remote_address
    from slowapi.errors import RateLimitExceeded
    _SLOWAPI_AVAILABLE = True
except ImportError:
    _SLOWAPI_AVAILABLE = False

import sqlalchemy as sa
from data.postgres.client import PostgresClient
from data.postgres.async_client import AsyncPostgresClient
from data.postgres.engine import get_async_engine
from nervous_system.notifications.telegram import send_telegram_message
from shared.config import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifespan — startup/shutdown hooks
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan context manager for startup/shutdown."""
    yield
    # Shutdown: dispose async engines to release connections
    engine = get_async_engine(settings.database_url)
    await engine.dispose()
    logger.info("Disposed async database engine")

app = FastAPI(
    title="Nervous System API",
    version="0.2.0",
    description="Knowledge base and orchestration API for the Second Brain",
    lifespan=lifespan,
    openapi_tags=[
        {"name": "health", "description": "Health checks"},
        {"name": "articles", "description": "Knowledge base articles"},
        {"name": "ingests", "description": "Ingest queue management"},
        {"name": "repos", "description": "GitHub repository ingestion"},
        {"name": "skills", "description": "Agent skills extracted from repos"},
        {"name": "flows", "description": "Prefect flow dispatch"},
        {"name": "ops", "description": "Operational endpoints"},
        {"name": "private", "description": "Private content ingestion"},
    ],
)


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

def get_db() -> AsyncPostgresClient:
    """Dependency for async database client."""
    return AsyncPostgresClient(settings.database_url)


# Type alias for cleaner handler signatures
DB = Annotated[AsyncPostgresClient, Depends(get_db)]

# ---------------------------------------------------------------------------
# CORS Configuration
# ---------------------------------------------------------------------------
_CORS_ORIGINS = os.getenv("CORS_ALLOWED_ORIGINS", "").strip()
if _CORS_ORIGINS:
    _origins = [o.strip() for o in _CORS_ORIGINS.split(",") if o.strip()]
else:
    _origins = []  # No CORS headers added — same-origin only

if _origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["*"],
    )

# ---------------------------------------------------------------------------
# Rate Limiting
# ---------------------------------------------------------------------------
_RATE_LIMIT = os.getenv("API_RATE_LIMIT", "60/minute").strip()

if _SLOWAPI_AVAILABLE:
    limiter = Limiter(key_func=get_remote_address, default_limits=[_RATE_LIMIT])
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
else:
    limiter = None
    logger.warning("slowapi not installed — rate limiting disabled")

# ---------------------------------------------------------------------------
# API Key Authentication
# ---------------------------------------------------------------------------
_API_KEY = os.getenv("API_SECRET_KEY", "").strip()


@app.middleware("http")
async def check_api_key_middleware(request: Request, call_next: Any) -> Any:
    """Enforce API key on all requests except health check.
    
    If API_SECRET_KEY is not set, auth is disabled (dev mode).
    """
    # Health endpoint is always open
    if request.url.path == "/health":
        return await call_next(request)
    # OPTIONS for CORS preflight
    if request.method == "OPTIONS":
        return await call_next(request)
    if not _API_KEY:
        # Dev mode — no auth required
        return await call_next(request)
    
    api_key = request.headers.get("X-API-Key", "")
    if api_key != _API_KEY:
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=401, content={"detail": "Invalid or missing API key"})
    
    return await call_next(request)

# ---------------------------------------------------------------------------
# OpenTelemetry instrumentation — enabled when OTEL_EXPORTER_OTLP_ENDPOINT set
# ---------------------------------------------------------------------------
_otel_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
if _otel_endpoint:
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.sdk.resources import Resource, SERVICE_NAME
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry.instrumentation.requests import RequestsInstrumentor

        _resource = Resource.create({
            SERVICE_NAME: os.getenv("OTEL_SERVICE_NAME", "nervous-system-api"),
            "deployment.environment": "local",
            "service.namespace": "provision",
        })
        _provider = TracerProvider(resource=_resource)
        _exporter = OTLPSpanExporter(endpoint=_otel_endpoint, insecure=True)
        _provider.add_span_processor(BatchSpanProcessor(_exporter))
        trace.set_tracer_provider(_provider)

        FastAPIInstrumentor.instrument_app(app)
        RequestsInstrumentor().instrument()

        logger.info("OpenTelemetry tracing enabled → %s", _otel_endpoint)
    except ImportError:
        logger.warning("opentelemetry packages not installed — tracing disabled")
    except Exception as exc:
        logger.warning("OpenTelemetry setup failed: %s — continuing without tracing", exc)


# ---------------------------------------------------------------------------
# Request/Response Models
# ---------------------------------------------------------------------------

class APIModel(BaseModel):
    """Base model with shared config."""
    model_config = ConfigDict(from_attributes=True)


# --- Request Models ---

class NotifyTarget(APIModel):
    """Notification routing for async operations."""
    context: str | None = Field(None, description="Notify context name (maps to agent config)")
    channel: str | None = Field(None, description="Channel override (telegram, discord, etc.)")
    account_id: str | None = Field(None, description="Account ID for multi-account setups")
    chat_id: str | None = Field(None, description="Direct chat/channel ID")
    reply_to_message_id: str | None = Field(None, description="Message ID to reply to")


class SaveContentRequest(APIModel):
    """Request to ingest a URL into the knowledge base."""
    url: str = Field(..., min_length=1, description="URL to ingest", examples=["https://example.com/article"])
    force: bool = Field(False, description="Re-ingest even if URL already exists")
    notify: NotifyTarget | None = Field(None, description="Notification routing for completion")


class ReingestRequest(APIModel):
    """Request to re-ingest articles matching a query."""
    query: str = Field(..., description="Search query to find articles", examples=["prefect workflow"])
    limit: int = Field(10, ge=1, le=100, description="Maximum articles to re-ingest")


class RepoIngestRequest(APIModel):
    """Request to ingest a GitHub repository."""
    url: str = Field(..., description="GitHub repo URL", examples=["https://github.com/prefecthq/prefect"])
    force: bool = Field(False, description="Re-ingest even if repo already exists")


class RepoCheckUpdatesRequest(APIModel):
    """Request to check a GitHub repo for updates."""
    url: str = Field(..., description="GitHub repo URL to check", examples=["https://github.com/prefecthq/prefect"])


class RepoUpdateRequest(APIModel):
    """Request to partially update a repo record."""
    our_notes: str | None = Field(None, description="Agent/human annotation about this repo")
    watched: bool | None = Field(None, description="Enable release tracking for this repo")


class ResearchRequest(APIModel):
    """Request to research a topic."""
    query: str = Field(..., description="Research query", examples=["what is context engineering"])
    num_sources: int = Field(5, ge=1, le=20, description="Number of sources to fetch and synthesize")


class DrainRequest(APIModel):
    """Request to drain a Prefect work pool."""
    work_pool: str = Field("default-pool", description="Work pool to drain")
    cancel_running: bool = Field(True, description="Also cancel RUNNING flow runs (not just PENDING). SCHEDULED runs are never cancelled.")
    dry_run: bool = Field(False, description="Preview what would be cancelled without doing it")


class IngestTextRequest(APIModel):
    """Request to ingest raw text content."""
    content: str = Field(..., description="Text content to ingest")
    title: str = Field("", description="Optional title")
    source_type: str = Field("note", description="Content type: note, voice, extracted")
    contributor: str = Field("user", description="Who contributed this content — e.g. 'user', 'agent', or a specific agent name")
    privacy: str = Field("private", description="Visibility: public or private")
    tags: str = Field("", description="Comma-separated tags")


class TwitterFollowRequest(APIModel):
    """Request to follow/unfollow a Twitter user."""
    username: str = Field(..., description="Twitter username to follow/unfollow")


class MedicineAckRequest(APIModel):
    """Request to acknowledge a medicine reminder."""
    window: str = Field(..., description="Reminder window: morning or afternoon")


class WriteArticleRequest(APIModel):
    """Request to trigger the iterative article writing flow."""
    topic: str = Field(..., description="Topic to write about")
    angle: str = Field("", description="Specific angle or perspective")
    format: str = Field("thread", description="Output format: thread, blog, essay")
    drafts: int = Field(3, ge=1, le=10, description="Number of drafts to generate")
    edit_rounds: int = Field(2, ge=0, le=5, description="Number of edit iterations")
    output_path: str | None = Field(None, description="Custom output path (defaults to config)")
    use_second_brain: bool = Field(True, description="Pull context from second brain")
    notify: bool = Field(True, description="Send Telegram notification on completion")


class PostTweetRequest(APIModel):
    """Request to draft and post a tweet."""
    days: int = Field(7, ge=1, le=30, description="Days of content to consider")
    article_limit: int = Field(20, ge=1, le=100, description="Max articles to pull")
    draft_count: int = Field(5, ge=1, le=10, description="Number of drafts to generate")
    dry_run: bool = Field(False, description="Draft without posting")
    text: str | None = Field(None, description="Override text (skip drafting)")


# --- Response Models ---

class HealthResponse(APIModel):
    """Health check response."""
    status: str = Field(..., description="Service status", examples=["ok"])


class ArticleResponse(APIModel):
    """A knowledge base article."""
    id: str | None = Field(None, description="Unique article ID")
    url: str = Field(..., description="Source URL")
    title: str | None = Field(None, description="Article title")
    summary: str | None = Field(None, description="LLM-generated summary")
    tags: list[str] | None = Field(default_factory=list, description="Auto-generated tags")
    source_type: str | None = Field(None, description="Content type: article, video, tweet, github, etc.")
    status: str | None = Field(None, description="Processing status: pending, processed, failed")
    ingested_at: _datetime | None = Field(None, description="When the article was first seen")
    processed_at: _datetime | None = Field(None, description="When LLM analysis completed")
    score_usefulness: int | None = Field(None, ge=1, le=5, description="Practical value score (1-5)")
    score_interest: int | None = Field(None, ge=1, le=5, description="Interest/engagement score (1-5)")
    score_pov: int | None = Field(None, ge=1, le=5, description="Strength of perspective (1-5)")
    score_uniqueness: int | None = Field(None, ge=1, le=5, description="Novelty/uniqueness score (1-5)")
    privacy: str | None = Field(None, description="Visibility: public or private")


class ArticleLookupResponse(APIModel):
    """Response for single-article lookup by URL."""
    found: bool = Field(..., description="Whether the article exists")
    article: ArticleResponse | None = Field(None, description="Article data if found")


class RecentFailure(APIModel):
    """A recent article processing failure."""
    url: str = Field(..., description="Article URL that failed")
    processed_at: _datetime | None = Field(None, description="When processing failed")
    failure_log: list[str | dict] | None = Field(None, description="Error messages")





class ArticleStatsResponse(APIModel):
    """Aggregate statistics for the knowledge base."""
    counts: dict[str, int] = Field(..., description="Article counts by status")
    recent_failures: list[RecentFailure] = Field(default_factory=list, description="Recent processing failures")


class TagCount(APIModel):
    """Tag frequency in the knowledge base."""
    tag: str = Field(..., description="Tag name")
    count: int = Field(..., description="Number of articles with this tag")


class IngestResponse(APIModel):
    """An ingest queue entry."""
    id: str = Field(..., description="Ingest ID")
    url: str = Field(..., description="URL being ingested")
    status: str = Field(..., description="Ingest status: pending, processing, completed, failed")
    created_at: _datetime | None = Field(None, description="When the ingest was created")
    completed_at: _datetime | None = Field(None, description="When the ingest finished")
    flow_run_id: str | None = Field(None, description="Prefect flow run ID if dispatched")
    destination: str | None = Field(None, description="Where content was routed (articles, repos, etc.)")
    error: str | None = Field(None, description="Error message if ingest failed")


class IngestCreatedResponse(APIModel):
    """Response when a new ingest is queued."""
    status: str = Field(..., description="Request status", examples=["processing", "queued"])
    url: str = Field(..., description="URL being ingested")
    ingest_id: str | None = Field(None, description="Ingest record ID")
    job_id: str | None = Field(None, description="Legacy job tracking ID (fallback path)")
    flow_run_id: str | None = Field(None, description="Prefect flow run ID")
    message: str | None = Field(None, description="Human-readable status message")


class IngestCompleteResponse(APIModel):
    """Response for marking an ingest complete."""
    status: str = Field(..., description="Operation status")
    ingest_id: str = Field(..., description="Ingest ID")
    destination: str = Field(..., description="Where the content was routed")


class RepoResponse(APIModel):
    """An ingested GitHub repository."""
    id: str | None = Field(None, description="Unique repo ID")
    url: str = Field(..., description="GitHub repo URL")
    owner: str | None = Field(None, description="Repository owner")
    name: str | None = Field(None, description="Repository name")
    description: str | None = Field(None, description="GitHub description")
    stars: int | None = Field(None, description="Star count at time of ingest")
    purpose: str | None = Field(None, description="LLM-derived purpose summary")
    architecture: str | None = Field(None, description="LLM-derived architecture notes")
    key_features: list[str] | None = Field(default_factory=list, description="Key features identified")
    stack: list[str] | None = Field(default_factory=list, description="Technology stack")
    tradeoffs: str | None = Field(None, description="Notable tradeoffs or limitations")
    fit_for_us: str | None = Field(None, description="Relevance assessment")
    our_notes: str | None = Field(None, description="Human-added notes")
    watched: bool = Field(False, description="Whether to track releases")
    status: str | None = Field(None, description="Processing status")
    last_release: str | None = Field(None, description="Last known release tag")
    ingested_at: _datetime | None = Field(None, description="When the repo was ingested")


class RepoUpdateResponse(APIModel):
    """Response for repo update operations."""
    status: str = Field(..., description="Operation status: updated, no_changes, error")
    id: str = Field(..., description="Repo ID")
    url: str | None = Field(None, description="Repo URL")
    fields: list[str] = Field(default_factory=list, description="Fields that were updated")
    error: str | None = Field(None, description="Error message if failed")


class RepoIngestResponse(APIModel):
    """Response when a repo ingest is queued."""
    url: str = Field(..., description="GitHub repo URL")
    status: str = Field(..., description="Request status: processing, error")
    flow_run_id: str | None = Field(None, description="Prefect flow run ID")
    message: str | None = Field(None, description="Human-readable status message")


class SkillResponse(APIModel):
    """An agent skill extracted from a repository."""
    id: str | None = Field(None, description="Unique skill ID")
    repo_id: str | None = Field(None, description="Source repository ID")
    name: str = Field(..., description="Skill name")
    description: str | None = Field(None, description="Skill description")
    skill_path: str | None = Field(None, description="Path to SKILL.md in repo")
    source_url: str | None = Field(None, description="GitHub URL to skill file")
    install_cmd: str | None = Field(None, description="Installation command if specified")
    ingested_at: _datetime | None = Field(None, description="When the skill was extracted")


class DispatchResponse(APIModel):
    """Response when a Prefect flow is dispatched."""
    status: str = Field(..., description="Dispatch status: dispatched, error", examples=["dispatched"])
    flow_run_id: str | None = Field(None, description="Prefect flow run ID")
    message: str | None = Field(None, description="Human-readable status message")


class ReingestDispatchedItem(APIModel):
    """A single article queued for re-ingestion."""
    url: str = Field(..., description="Article URL")
    flow_run_id: str | None = Field(None, description="Prefect flow run ID")


class ReingestError(APIModel):
    """An error during re-ingest dispatch."""
    url: str = Field(..., description="Article URL that failed")
    error: str = Field(..., description="Error message")


class ReingestResponse(APIModel):
    """Response for bulk re-ingest operation."""
    matched: int = Field(..., description="Number of articles matching query")
    dispatched: list[ReingestDispatchedItem] = Field(default_factory=list, description="Successfully queued articles")
    errors: list[ReingestError] = Field(default_factory=list, description="Failed dispatches")
    message: str | None = Field(None, description="Summary message")


class IngestTextResponse(APIModel):
    """Response for text ingestion."""
    status: str = Field(..., description="Operation status")
    url: str = Field(..., description="Synthetic URL for the stored content")
    privacy: str = Field(..., description="Content visibility")


class DrainResponse(APIModel):
    """Response for work pool drain operation."""
    status: str = Field(..., description="Operation status: drained, dry_run, nothing_to_drain")
    work_pool: str = Field(..., description="Work pool that was drained")
    cancelled: int = Field(0, description="Number of flow runs cancelled")
    by_state: dict[str, int] = Field(default_factory=dict, description="Cancelled counts by state")
    errors: list[dict] = Field(default_factory=list, description="Errors during cancellation")
    would_cancel: dict[str, int] | None = Field(None, description="What would be cancelled (dry_run only)")
    total: int | None = Field(None, description="Total runs affected (dry_run only)")


# ---------------------------------------------------------------------------
# Private endpoint response models
# ---------------------------------------------------------------------------

class MedicineAckResponse(APIModel):
    """Response for medicine acknowledgement."""
    status: str = Field(..., description="Operation status")
    window: str | None = Field(None, description="Window acknowledged")
    message: str | None = Field(None, description="Result message")


class MedicineCheckResponse(APIModel):
    """Response for medicine reminder check."""
    status: str = Field(..., description="Operation status")
    message: str | None = Field(None, description="Result message")
    needs_reminder: bool | None = Field(None, description="Whether reminder was sent")


class TweetResponse(APIModel):
    """Response for tweet operations."""
    status: str = Field(..., description="Operation status")
    tweet_id: str | None = Field(None, description="Tweet ID if posted")
    text: str | None = Field(None, description="Tweet text")
    dry_run: bool = Field(False, description="Whether this was a dry run")


class TwitterUserResponse(APIModel):
    """Response for Twitter follow/unfollow operations."""
    success: bool = Field(..., description="Whether operation succeeded")
    username: str = Field(..., description="Target username")
    user_id: str | None = Field(None, description="Twitter user ID")
    error: str | None = Field(None, description="Error message if failed")


class TwitterFollowingResponse(APIModel):
    """Response for Twitter following list."""
    count: int = Field(..., description="Number of users returned")
    following: list[dict] = Field(default_factory=list, description="List of followed users")


class ImageIngestResponse(APIModel):
    """Response for image ingestion."""
    status: str = Field(..., description="Operation status")
    url: str = Field(..., description="Synthetic URL for the stored content")
    file_path: str | None = Field(None, description="MinIO path if stored")
    privacy: str = Field(..., description="Content visibility")


class WriteArticleResponse(APIModel):
    """Response for article writing dispatch."""
    status: str = Field(..., description="Operation status")
    topic: str = Field(..., description="Article topic")
    message: str | None = Field(None, description="Status message")


class TestNotificationResponse(APIModel):
    """Response for notification test endpoint."""
    telegram: str = Field(..., description="Telegram test result")
    database: str | None = Field(None, description="Database test result")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
# Note on inline imports: several handlers import `trigger_deployment`,
# flow modules, and integration clients inline rather than at module level.
# This is intentional — it avoids importing heavy Prefect/flow dependencies
# at startup (which would slow cold starts and cause errors if optional deps
# are missing), and prevents circular imports between server and flow modules.

@app.get("/health", tags=["health"], response_model=HealthResponse)
async def health() -> HealthResponse:
    return HealthResponse(status="ok")


# ---------------------------------------------------------------------------
# Article read endpoints
# ---------------------------------------------------------------------------




@app.get("/articles", tags=["articles"], response_model=list[ArticleResponse])
async def list_articles(
    db: DB,
    q: str | None = Query(None, description="Full-text / semantic search query"),
    url: str | None = Query(None, description="Exact URL lookup"),
    status: str | None = Query(None, description="Filter by status: processed, failed, pending"),
    since: _datetime | None = Query(None, description="ISO timestamp lower bound, e.g. 2026-03-13T19:00:00"),
    tags: str | None = Query(None, description="Comma-separated tag filter (AND semantics), e.g. agent,prefect"),
    score_min: int | None = Query(None, ge=1, le=5, description="Min score_usefulness"),
    source_type: str | None = Query(None, description="Filter by source type: tweet, article, youtube, etc."),
    include_private: bool = Query(True),
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> list[ArticleResponse]:
    """List or search articles. All filters are composable.

    - `GET /articles` → recent articles
    - `GET /articles?q=<query>` → semantic/FTS search
    - `GET /articles?url=<url>` → exact URL lookup
    - `GET /articles?status=failed&since=2026-03-01` → filtered list
    - `GET /articles?tags=agent,prefect&score_min=4` → high-quality agent articles
    """
    embedding: list[float] | None = None
    if q:
        try:
            from integrations.ollama import OllamaClient
            ollama = OllamaClient(settings.ollama_base_url)
            embedding = ollama.embed(q)
        except Exception as exc:
            logger.warning("Embedding failed, falling back to FTS: %s", exc)

    tag_list = [t.strip() for t in tags.split(",")] if tags else None

    results = await db.list_articles(
        q=q,
        url=url,
        status=status,
        since=since,
        tags=tag_list,
        score_min=score_min,
        source_type=source_type,
        include_private=include_private,
        limit=limit,
        offset=offset,
        embedding=embedding,
    )
    return [ArticleResponse.model_validate(r) for r in results]


@app.get("/ingests", tags=["ingests"], response_model=list[IngestResponse])
async def list_ingests(
    db: DB,
    status: str | None = Query(None, description="Filter by status: pending, processing, completed, failed"),
    limit: int = Query(20, ge=1, le=100),
) -> list[IngestResponse]:
    """List ingest queue records, newest first."""
    results = await db.get_recent_ingests(limit=limit, status=status)
    return [IngestResponse(**r) for r in results]


@app.get("/ingests/{ingest_id}", tags=["ingests"], response_model=IngestResponse)
async def get_ingest(ingest_id: str, db: DB) -> IngestResponse:
    """Get a single ingest record by ID."""
    record = await db.get_ingest(ingest_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"ingest {ingest_id} not found")
    return IngestResponse(**record)


@app.patch("/ingests/{ingest_id}/complete", tags=["ingests"], response_model=IngestCompleteResponse)
async def mark_ingest_complete(ingest_id: str, db: DB, destination: str = Query("articles")) -> IngestCompleteResponse:
    """Manually mark an ingest as completed (for stuck pending records)."""
    record = await db.get_ingest(ingest_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"ingest {ingest_id} not found")
    await db.complete_ingest(ingest_id, destination=destination)
    return IngestCompleteResponse(status="completed", ingest_id=ingest_id, destination=destination)


@app.get("/articles/tags", tags=["articles"], response_model=list[TagCount])
async def get_article_tags(db: DB, limit: int = Query(200, ge=1, le=500)) -> list[TagCount]:
    """Return top tags across all processed articles, ordered by frequency."""
    results = await db.get_top_tags(limit=limit)
    return [TagCount(tag=r["tag"], count=r["count"]) for r in results]


@app.get("/articles/stats", tags=["articles"], response_model=ArticleStatsResponse)
async def get_articles_stats(db: DB) -> ArticleStatsResponse:
    """Return article counts by status and the 5 most recent failures."""
    stats = await db.get_articles_stats()
    return ArticleStatsResponse(
        counts=stats["counts"],
        recent_failures=[RecentFailure(**f) for f in stats["recent_failures"]],
    )





@app.post("/articles/retry-failed", tags=["flows"], response_model=DispatchResponse)
async def retry_failed_articles(limit: int = Query(50, ge=1, le=200)) -> DispatchResponse:
    """Trigger the retry-failed Prefect flow to re-queue failed and stale-pending articles."""
    from orchestration.prefect.client import trigger_deployment_async
    flow_run_id = await trigger_deployment_async("retry-failed", "retry-failed", parameters={"limit": limit})
    return DispatchResponse(status="dispatched", flow_run_id=flow_run_id, message=f"limit={limit}")


@app.post("/articles/rescore", tags=["flows"], response_model=DispatchResponse)
async def rescore_articles_endpoint(limit: int = Query(100, ge=1, le=500)) -> DispatchResponse:
    """Trigger rescore-articles Prefect flow — re-runs LLM on articles missing scores/tags."""
    from orchestration.prefect.client import trigger_deployment_async
    flow_run_id = await trigger_deployment_async("rescore-articles", "rescore-articles", parameters={"limit": limit})
    return DispatchResponse(status="dispatched", flow_run_id=flow_run_id, message=f"limit={limit}")


from shared.url_utils import strip_tracking_params


@app.post("/ingest", tags=["ingest"], response_model=IngestCreatedResponse)
async def save_content(request: SaveContentRequest, db: DB) -> IngestCreatedResponse:
    """Submit a URL for ingestion into the knowledge base."""
    from orchestration.prefect.client import trigger_deployment_async

    url = strip_tracking_params(request.url)
    force = request.force
    notify = request.notify.model_dump(exclude_none=True) if request.notify else None

    # Create the ingests record immediately — before Prefect is involved.
    # This ensures we have a durable record regardless of what happens next.
    ingest_id = await db.create_ingest(url=url, notify=notify)

    try:
        flow_run_id = await trigger_deployment_async(
            "ingest-url", "ingest-url",
            parameters={"url": url, "force": force, "notify": notify or {}, "ingest_id": ingest_id},
        )

        # Attach the flow run ID to the ingest record
        await db.update_ingest_flow_run(ingest_id, flow_run_id=flow_run_id)

        return IngestCreatedResponse(
            url=url,
            status="processing",
            ingest_id=ingest_id,
            flow_run_id=flow_run_id,
            message="Queued — notification will be sent when done.",
        )
    except Exception as exc:
        error_msg = f"Prefect dispatch failed: {str(exc)[:400]}"
        logger.warning("Prefect dispatch failed for %s: %s", url, exc)
        await db.fail_ingest(ingest_id, error=error_msg)
        return IngestCreatedResponse(
            url=url,
            status="failed",
            ingest_id=ingest_id,
            message=error_msg,
        )


# ---------------------------------------------------------------------------
# Repos endpoints — GitHub repo ingestion
# ---------------------------------------------------------------------------

@app.post("/repos", tags=["repos"], response_model=RepoIngestResponse)
async def ingest_repo(request: RepoIngestRequest) -> RepoIngestResponse:
    """Ingest a GitHub repository into the second brain."""
    from integrations.github import is_github_repo_url
    from orchestration.prefect.client import trigger_deployment_async

    url = request.url
    force = request.force
    if not is_github_repo_url(url):
        raise HTTPException(status_code=422, detail="Not a valid GitHub repo URL")

    try:
        flow_run_id = await trigger_deployment_async(
            "ingest-github-repo", "ingest-github-repo",
            parameters={"url": url, "force": force},
        )
        return RepoIngestResponse(
            url=url,
            status="processing",
            flow_run_id=flow_run_id,
            message="Queued — notification will be sent when done.",
        )
    except Exception as exc:
        logger.error("Prefect dispatch failed for repo ingest: %s", exc)
        return RepoIngestResponse(
            url=url,
            status="error",
            message=f"Prefect unavailable: {str(exc)[:200]}",
        )


@app.get("/repos", tags=["repos"], response_model=list[RepoResponse])
async def list_repos(
    db: DB,
    q: str | None = Query(None, description="Full-text search query"),
    watched: bool | None = Query(None, description="Filter to watched repos only"),
    limit: int = Query(20, ge=1, le=100),
) -> list[RepoResponse]:
    """List or search ingested repos.

    - `GET /repos` — all repos, newest first
    - `GET /repos?q=prefect` — FTS search
    - `GET /repos?watched=true` — watched repos only
    """
    results = await db.list_repos(q=q, watched=watched, limit=limit)
    return [RepoResponse(**r) for r in results]


@app.patch("/repos/{repo_id}", tags=["repos"], response_model=RepoUpdateResponse)
async def update_repo(repo_id: str, request: RepoUpdateRequest, db: DB) -> RepoUpdateResponse:
    """Partial update for a repo record."""
    repo = await db.get_repo_by_id(repo_id)
    if not repo:
        raise HTTPException(status_code=404, detail=f"Repo {repo_id} not found")

    url = repo["url"]
    
    if request.our_notes is None and request.watched is None:
        return RepoUpdateResponse(status="no_changes", id=repo_id, url=url)

    await db.update_repo(url, our_notes=request.our_notes, watched=request.watched)
    
    fields = []
    if request.our_notes is not None:
        fields.append("our_notes")
    if request.watched is not None:
        fields.append("watched")
    return RepoUpdateResponse(status="updated", id=repo_id, url=url, fields=fields)


@app.post("/repos/check-updates", tags=["repos"], response_model=DispatchResponse)
async def check_repo_updates_endpoint(request: RepoCheckUpdatesRequest) -> DispatchResponse:
    """Dispatch check-repo-updates as a Prefect deployment run (fire-and-forget).

    Fetches new releases and README, runs Ollama analysis, and updates the record if changed.
    """
    from orchestration.prefect.client import trigger_deployment_async
    try:
        flow_run_id = await trigger_deployment_async("check-repo-updates", "check-repo-updates", parameters={"url": request.url})
        return DispatchResponse(status="dispatched", flow_run_id=flow_run_id)
    except Exception as exc:
        return DispatchResponse(status="error", message=str(exc)[:200])


@app.get("/skills", tags=["skills"], response_model=list[SkillResponse])
async def search_skills(
    db: DB,
    q: str | None = Query(None, description="FTS search across skill name and description"),
    repo_id: str | None = Query(None, description="Filter skills by repo UUID"),
    limit: int = Query(20, ge=1, le=100),
) -> list[SkillResponse]:
    """List or search skills found in ingested repos.

    - `GET /skills` — all skills, alphabetical
    - `GET /skills?q=installer` — FTS search
    - `GET /skills?repo_id=<uuid>` — skills for a specific repo
    """
    if repo_id:
        results = await db.get_skills_for_repo(repo_id)
    elif q:
        results = await db.search_skills(q, limit=limit)
    else:
        results = await db.list_skills(limit=limit)
    return [SkillResponse(**r) for r in results]



@app.post("/ops/test-notification", tags=["ops"], response_model=TestNotificationResponse)
async def test_notification() -> TestNotificationResponse:
    """Debug: test Telegram notification from within this container."""
    from shared.secrets import load_telegram_credentials
    
    telegram_result = "not tested"

    try:
        creds = load_telegram_credentials()
        if creds:
            send_telegram_message(
                creds.bot_token,
                creds.chat_id,
                "🔧 Notification test from nervous_system API container",
            )
            telegram_result = "sent"
        else:
            telegram_result = "missing creds: telegram-credentials block not found"
    except Exception as exc:
        telegram_result = f"error: {exc}"

    return TestNotificationResponse(telegram=telegram_result)


@app.post("/sync/readwise", tags=["flows"], response_model=DispatchResponse)
async def run_sync_readwise(force_full: bool = False) -> DispatchResponse:
    """Dispatch sync-readwise as a Prefect deployment run (fire-and-forget)."""
    from orchestration.prefect.client import trigger_deployment_async
    flow_run_id = await trigger_deployment_async("sync-readwise", "sync-readwise", parameters={"force_full": force_full})
    return DispatchResponse(status="dispatched", flow_run_id=flow_run_id)


@app.post("/digest", tags=["flows"], response_model=DispatchResponse)
async def run_weekly_digest(days: int = 7) -> DispatchResponse:
    """Dispatch weekly-digest as a Prefect deployment run (fire-and-forget)."""
    from orchestration.prefect.client import trigger_deployment_async
    flow_run_id = await trigger_deployment_async("weekly-digest", "weekly-digest", parameters={"days": days})
    return DispatchResponse(status="dispatched", flow_run_id=flow_run_id)


@app.post("/ops/run-tests", tags=["ops"], response_model=DispatchResponse)
async def run_tests_endpoint(
    path: str = Query("tests/", description="Test path relative to app root"),
    extra_args: list[str] | None = Query(None, description="Additional pytest args"),
    notify: bool = Query(True, description="Send Telegram notification when done"),
) -> DispatchResponse:
    """Dispatch the run-tests Prefect flow — runs pytest inside the worker container."""
    from orchestration.prefect.client import trigger_deployment_async  # Inline: avoid startup cost
    flow_run_id = await trigger_deployment_async("run-tests", "run-tests", parameters={
        "path": path,
        "extra_args": extra_args or [],
        "notify": notify,
    })
    return DispatchResponse(status="dispatched", flow_run_id=flow_run_id)


@app.get("/ops/ollama-slot", tags=["ops"])
async def ops_ollama_slot() -> dict:
    """Return info on which flow run currently holds the ollama concurrency slot."""
    import httpx
    from orchestration.prefect.client import PREFECT_API_URL
    async with httpx.AsyncClient() as client:
        # Check active slot count
        limits_resp = await client.post(
            f"{PREFECT_API_URL}/v2/concurrency_limits/filter",
            json={}, timeout=10,
        )
        limits = limits_resp.json()
        ollama_limit = next((l for l in limits if l["name"] == "ollama"), None)
        active_slots = ollama_limit.get("active_slots", 0) if ollama_limit else 0

        # Fetch last known holder from Prefect Variables
        var_resp = await client.get(
            f"{PREFECT_API_URL}/variables/name/ollama-slot-holder",
            timeout=10,
        )
        holder = None
        if var_resp.status_code == 200:
            try:
                import json as _json
                holder = _json.loads(var_resp.json().get("value", "{}"))
            except Exception:
                pass

    return {
        "active_slots": active_slots,
        "limit": ollama_limit.get("limit", 1) if ollama_limit else 1,
        "current_holder": holder,
    }


@app.post("/ops/drain", tags=["ops"], response_model=DrainResponse)
async def ops_drain(request: DrainRequest) -> DrainResponse:
    """Drain the Prefect work pool: cancel all PENDING (and optionally RUNNING) flow runs.

    Use before restarting the worker to avoid zombie flow runs and leaked concurrency slots.

    Returns counts of cancelled runs by state.
    """
    import httpx
    from orchestration.prefect.client import PREFECT_API_URL

    work_pool = request.work_pool
    cancel_running = request.cancel_running
    dry_run = request.dry_run

    # SCHEDULED = waiting for cron time — never cancel, they'll fire naturally
    # PENDING = dispatched but not yet picked up by worker — cancel these
    states = ["PENDING"]
    if cancel_running:
        states.append("RUNNING")

    async with httpx.AsyncClient(timeout=15) as client:
        # Find all flow runs in target states.
        # Note: Prefect 3.x flow_runs/filter does not support work_pool_name as a filter field —
        # filter by state only, then optionally filter by work_pool client-side if needed.
        search_resp = await client.post(
            f"{PREFECT_API_URL}/flow_runs/filter",
            json={
                "flow_runs": {
                    "state": {"type": {"any_": states}},
                },
                "limit": 200,
            },
        )
        search_resp.raise_for_status()
        flow_runs = search_resp.json()

        if not flow_runs:
            return DrainResponse(status="nothing_to_drain", work_pool=work_pool, cancelled=0)

        by_state: dict[str, list[str]] = {}
        for run in flow_runs:
            state = run["state_type"]
            by_state.setdefault(state, []).append(run["id"])

        if dry_run:
            return DrainResponse(
                status="dry_run",
                work_pool=work_pool,
                would_cancel={k: len(v) for k, v in by_state.items()},
                total=len(flow_runs),
            )

        # Cancel each run
        cancelled = 0
        errors: list[dict] = []
        for run in flow_runs:
            try:
                cancel_resp = await client.post(
                    f"{PREFECT_API_URL}/flow_runs/{run['id']}/set_state",
                    json={"state": {"type": "CANCELLED", "name": "Cancelled", "message": "Drained via /ops/drain"}},
                )
                cancel_resp.raise_for_status()
                cancelled += 1
            except Exception as exc:
                errors.append({"id": run["id"], "error": str(exc)[:100]})

        # Reset ollama concurrency slot — delete and recreate to clear leaked active_slots
        # Uses v2 API which is what our concurrency limits live on
        try:
            limits_resp = await client.post(f"{PREFECT_API_URL}/v2/concurrency_limits/filter", json={})
            if limits_resp.status_code == 200:
                for limit in limits_resp.json():
                    if limit.get("active_slots", 0) > 0:
                        lid = limit["id"]
                        name = limit["name"]
                        lim = limit["limit"]
                        await client.delete(f"{PREFECT_API_URL}/v2/concurrency_limits/{lid}")
                        await client.post(f"{PREFECT_API_URL}/v2/concurrency_limits/", json={"name": name, "limit": lim})
        except Exception:
            pass  # Non-critical — slots will expire naturally

    return DrainResponse(
        status="drained",
        work_pool=work_pool,
        cancelled=cancelled,
        by_state={k: len(v) for k, v in by_state.items()},
        errors=errors,
    )


@app.post("/medicine/ack", tags=["private"], response_model=MedicineAckResponse)
async def run_medicine_ack(request: MedicineAckRequest) -> MedicineAckResponse:
    """Acknowledge a medicine reminder window (morning or afternoon).
    Called by agent when user taps the ✅ Taken button.
    """
    import asyncio
    from orchestration.flows.medicine_reminder import medicine_reminder_ack  # Inline: avoids Prefect import at startup
    result = await asyncio.to_thread(medicine_reminder_ack, window=request.window)
    return MedicineAckResponse(status="ok", window=request.window, message=result.get("message"))


@app.post("/medicine/check", tags=["private"], response_model=MedicineCheckResponse)
async def run_medicine_check() -> MedicineCheckResponse:
    """Manually trigger a medicine reminder check."""
    import asyncio
    from orchestration.flows.medicine_reminder import medicine_reminder_check  # Inline: avoids Prefect import at startup
    result = await asyncio.to_thread(medicine_reminder_check)
    return MedicineCheckResponse(status="ok", message=result.get("message"), needs_reminder=result.get("needs_reminder"))


@app.post("/twitter/tweet", tags=["private"], response_model=TweetResponse)
async def run_post_tweet(request: PostTweetRequest) -> TweetResponse:
    """Draft and post a tweet from recent second brain content.

    Set dry_run=true to draft without posting.
    """
    from orchestration.prefect.client import trigger_deployment_async
    try:
        await trigger_deployment_async("post-tweet", "post-tweet", parameters={
            "days": request.days,
            "article_limit": request.article_limit,
            "draft_count": request.draft_count,
            "dry_run": request.dry_run,
            "text": request.text,
        })
        return TweetResponse(status="queued", tweet_id=None, text=None, dry_run=request.dry_run)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Failed to queue tweet flow: {e}")


def _get_twitter_client() -> Any:
    """Create TwitterClient from Prefect block credentials."""
    from integrations.twitter import TwitterClient
    from shared.secrets import load_twitter_credentials
    
    creds = load_twitter_credentials()
    if not creds:
        raise HTTPException(status_code=503, detail="Twitter credentials not configured (twitter-credentials block)")
    
    return TwitterClient(
        api_key=creds.api_key,
        api_secret=creds.api_secret,
        access_token=creds.access_token,
        access_token_secret=creds.access_token_secret,
    )


@app.post("/twitter/follow", tags=["private"], response_model=TwitterUserResponse)
async def twitter_follow(request: TwitterFollowRequest) -> TwitterUserResponse:
    """Follow a Twitter user by username."""
    import asyncio
    client = _get_twitter_client()
    result = await asyncio.to_thread(client.follow_user, username=request.username)
    return TwitterUserResponse(success=result.get("success", True), username=request.username, user_id=result.get("user_id"))


@app.post("/twitter/unfollow", tags=["private"], response_model=TwitterUserResponse)
async def twitter_unfollow(request: TwitterFollowRequest) -> TwitterUserResponse:
    """Unfollow a Twitter user by username."""
    import asyncio
    client = _get_twitter_client()
    result = await asyncio.to_thread(client.unfollow_user, username=request.username)
    return TwitterUserResponse(success=result.get("success", True), username=request.username, user_id=result.get("user_id"))


@app.get("/twitter/following", tags=["private"], response_model=TwitterFollowingResponse)
async def twitter_following(max_results: int = 100) -> TwitterFollowingResponse:
    """Get list of users the configured account is following."""
    import asyncio
    client = _get_twitter_client()
    users = await asyncio.to_thread(client.get_following, max_results=max_results)
    return TwitterFollowingResponse(count=len(users), following=users)


@app.post("/research", tags=["flows"], response_model=DispatchResponse)
async def run_research(request: ResearchRequest) -> DispatchResponse:
    """Dispatch research-topic as a Prefect deployment run (fire-and-forget)."""
    from orchestration.prefect.client import trigger_deployment_async
    flow_run_id = await trigger_deployment_async("research-topic", "research-topic", parameters={
        "query": request.query,
        "num_sources": request.num_sources,
    })
    return DispatchResponse(status="dispatched", flow_run_id=flow_run_id)


@app.post("/ingest/reprocess", tags=["ingest"], response_model=ReingestResponse)
async def reingest_by_query(request: ReingestRequest, db: DB) -> ReingestResponse:
    """Search for articles matching query and re-ingest them with force=True.

    Dispatches through POST /ingest (same path as normal ingestion).
    Returns immediately with the list of URLs queued.
    """
    from orchestration.prefect.client import trigger_deployment_async

    articles = await db.list_articles(q=request.query, limit=request.limit)

    if not articles:
        return ReingestResponse(matched=0, dispatched=[])

    dispatched: list[ReingestDispatchedItem] = []
    errors: list[ReingestError] = []

    for article in articles:
        url = article["url"]
        try:
            ingest_id = await db.create_ingest(url=url, notify=None)
            flow_run_id = await trigger_deployment_async(
                "ingest-url", "ingest-url",
                parameters={"url": url, "force": True, "notify": {}, "ingest_id": ingest_id},
            )
            await db.update_ingest_flow_run(ingest_id, flow_run_id=flow_run_id)
            dispatched.append(ReingestDispatchedItem(url=url, flow_run_id=flow_run_id))
        except Exception as exc:
            errors.append(ReingestError(url=url, error=str(exc)[:200]))

    return ReingestResponse(
        matched=len(articles),
        dispatched=dispatched,
        errors=errors,
        message=f"Queued {len(dispatched)} URLs for reingestion",
    )


# ---------------------------------------------------------------------------
# Private content ingestion (images, text, voice — stored locally)
# ---------------------------------------------------------------------------

@app.post("/ingest/image", tags=["private"], response_model=ImageIngestResponse)
async def save_image(
    db: DB,
    file: UploadFile = File(...),
    extracted_text: str = Form(...),
    title: str = Form(""),
    contributor: str = Form("user"),  # Caller should pass 'user', 'agent', or a specific agent name
    privacy: str = Form("private"),
) -> ImageIngestResponse:
    """Store a private image in MinIO and save extracted text to Postgres.
    
    The caller (agent) extracts text from the image via vision model,
    then POSTs here with both the raw file and the extracted content.
    Raw image goes to MinIO. Text goes to Postgres as a private article.
    """
    import uuid
    from data.storage import StorageClient

    # Store raw image in MinIO
    content_type = file.content_type or "image/jpeg"
    raw_bytes = await file.read()
    file_path = None
    try:
        storage = StorageClient()
        file_path = storage.store_file(raw_bytes, content_type, prefix="images")
    except Exception as e:
        logger.warning("MinIO storage failed: %s — storing text only", e)

    # Generate synthetic URL for the article
    synthetic_url = f"local://image/{uuid.uuid4().hex}"

    # Store in Postgres as private article
    await db.upsert_article(
        url=synthetic_url,
        readwise_id=None,
        title=title or "Screenshot",
        summary=extracted_text,
        tags=["screenshot", "private"],
        source_type="screenshot",
        raw_text=extracted_text,
        status="processed",
        privacy=privacy,
        contributor=contributor,
        file_path=file_path,
    )

    return ImageIngestResponse(status="stored", url=synthetic_url, file_path=file_path, privacy=privacy)


@app.post("/ingest/text", tags=["private"], response_model=IngestTextResponse)
async def save_text(request: IngestTextRequest, db: DB) -> IngestTextResponse:
    """Save raw text content (voice transcription, personal note, extracted text) as a private article."""
    import uuid as _u
    synthetic_url = f"local://{request.source_type}/{_u.uuid4().hex}"
    tag_list = [t.strip() for t in request.tags.split(",") if t.strip()] + [request.source_type, "private"]
    await db.upsert_article(
        url=synthetic_url,
        readwise_id=None,
        title=request.title or f"Personal {request.source_type}",
        summary=request.content[:500],
        tags=tag_list,
        source_type=request.source_type,
        raw_text=request.content,
        status="processed",
        privacy=request.privacy,
        contributor=request.contributor,
        file_path=None,
    )
    return IngestTextResponse(status="stored", url=synthetic_url, privacy=request.privacy)


@app.post("/write/article", tags=["private"], response_model=WriteArticleResponse)
async def write_article_endpoint(request: WriteArticleRequest) -> WriteArticleResponse:
    """
    Trigger the iterative article writing flow via Prefect.

    Qwen generates N drafts → Claude scores → best draft selected → 
    Claude/Qwen edit cycles → final voice pass → saved to disk.
    """
    from orchestration.prefect.client import trigger_deployment_async
    resolved_output_path = request.output_path or settings.article_drafts_path
    try:
        await trigger_deployment_async("write-article", "write-article", parameters={
            "topic": request.topic,
            "angle": request.angle,
            "format": request.format,
            "drafts": request.drafts,
            "edit_rounds": request.edit_rounds,
            "output_path": resolved_output_path,
            "use_second_brain": request.use_second_brain,
            "notify": request.notify,
        })
        return WriteArticleResponse(
            status="queued",
            topic=request.topic,
            message="Writing dispatched to Prefect — notification will be sent when done.",
        )
    except Exception as exc:
        return WriteArticleResponse(
            status="error",
            topic=request.topic,
            message=f"Prefect dispatch failed: {str(exc)[:200]}",
        )


def main() -> None:
    uvicorn.run("nervous_system.http.server:app", host="0.0.0.0", port=8001, reload=False)


if __name__ == "__main__":
    main()
