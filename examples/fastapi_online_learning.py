"""Online learning behind FastAPI: a feedback loop between predictions and outcomes.

POST /predict returns a prediction with an id. POST /feedback brings back the true label for
that id. A predictor actor micro-batches predictions into one numpy call per batch. A learner
actor keeps the recent predictions, scores incoming feedback, accumulates a minibatch, and
every few hundred milliseconds takes gradient steps on it in a thread, then hands the new
model to the predictor, which swaps it in between batches. The model starts untrained and
learns only from feedback.

    uv run --extra web --extra numpy python examples/fastapi_online_learning.py   # server + demo client
    uv run --extra web --extra numpy uvicorn examples.fastapi_online_learning:app --port 8000

    curl -X POST localhost:8000/predict  -H 'content-type: application/json' -d '{"features": [1.2, -0.3, 0.8, 2.1]}'
    curl -X POST localhost:8000/feedback -H 'content-type: application/json' -d '{"prediction_id": 1, "label": 1}'
    curl localhost:8000/model
"""
import asyncio
import itertools
import json
import sys
import urllib.request
from collections import OrderedDict
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any

import numpy as np
from fastapi import FastAPI
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pekko_py import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    Success,
    TimerScheduler,
    Try,
    batching,
)

N_FEATURES = 4
BATCH_MAX_N, BATCH_MAX_WAIT = 64, 0.005  # prediction micro-batches
STEP_EVERY, MIN_STEP_SAMPLES = 0.25, 8  # learner cadence
PENDING_LIMIT = 10_000  # predictions remembered while waiting for feedback

# --- model ------------------------------------------------------------------------------


@dataclass(frozen=True)
class Model:
    version: int
    weights: np.ndarray  # frozen view: writeable=False
    bias: float


def untrained() -> Model:
    w = np.zeros(N_FEATURES)
    w.flags.writeable = False
    return Model(0, w, 0.0)


def sgd_steps(model: Model, x: np.ndarray, y: np.ndarray, steps: int = 20, lr: float = 0.5) -> Model:
    """Logistic regression gradient steps on a minibatch. Runs in a thread."""
    w, b = model.weights.copy(), model.bias
    for _ in range(steps):
        p = 1 / (1 + np.exp(-(x @ w + b)))
        w -= lr * x.T @ (p - y) / len(y)
        b -= lr * float((p - y).mean())
    w.flags.writeable = False
    return Model(model.version + 1, w, b)


# --- predictor: batches requests, one matmul per batch, reports each prediction to the learner --


@dataclass(frozen=True)
class Predict:
    features: tuple[float, ...]
    reply_to: ActorRef["Prediction"]


@dataclass(frozen=True)
class Prediction:
    prediction_id: int
    probability: float
    model_version: int


@dataclass(frozen=True)
class UseModel:
    model: Model


type PredictorCmd = Predict | UseModel


def predictor(learner: ActorRef["Predicted"]) -> Behavior[PredictorCmd]:
    def setup(_ctx: ActorContext[PredictorCmd]) -> Behavior[PredictorCmd]:
        current = [untrained()]  # the batching handler is a closure, so the live model lives in a cell
        ids = itertools.count(1)

        def handle(batch: list[PredictorCmd]) -> None:
            requests: list[Predict] = []
            for msg in batch:
                match msg:
                    case UseModel(model):
                        flush(requests)  # requests queued before the swap use the old model
                        requests = []
                        current[0] = model
                    case Predict():
                        requests.append(msg)
            flush(requests)

        def flush(requests: list[Predict]) -> None:
            if not requests:
                return
            model = current[0]
            x = np.array([r.features for r in requests])
            probs = 1 / (1 + np.exp(-(x @ model.weights + model.bias)))  # one call for the whole batch
            for r, p in zip(requests, probs.tolist()):
                pid = next(ids)
                r.reply_to.tell(Prediction(pid, float(p), model.version))
                learner.tell(Predicted(pid, r.features, float(p)))

        return batching(BATCH_MAX_N, BATCH_MAX_WAIT, handle)

    return Behaviors.setup(setup)


# --- learner: closes the loop --------------------------------------------------------------------


@dataclass(frozen=True)
class Predicted:
    prediction_id: int
    features: tuple[float, ...]
    probability: float


@dataclass(frozen=True)
class Feedback:
    prediction_id: int
    label: int
    reply_to: ActorRef["FeedbackResult"]


@dataclass(frozen=True)
class FeedbackResult:
    accepted: bool  # False when the id is unknown or already used
    online_accuracy: float
    model_version: int


@dataclass(frozen=True)
class GetModel:
    reply_to: ActorRef["ModelInfo"]


@dataclass(frozen=True)
class ModelInfo:
    version: int
    feedback_count: int
    online_accuracy: float
    weights: tuple[float, ...]


@dataclass(frozen=True)
class _Step:
    pass


@dataclass(frozen=True)
class _Stepped:
    result: Try[Model]


type LearnerCmd = Predicted | Feedback | GetModel | _Step | _Stepped


@dataclass(frozen=True)
class Stats:
    feedback: int = 0
    correct: int = 0

    @property
    def accuracy(self) -> float:
        return self.correct / self.feedback if self.feedback else 0.0


def learner(predict_ref: Callable[[], ActorRef[UseModel]]) -> Behavior[LearnerCmd]:
    def with_timers(timers: TimerScheduler[LearnerCmd]) -> Behavior[LearnerCmd]:
        def setup(ctx: ActorContext[LearnerCmd]) -> Behavior[LearnerCmd]:
            timers.start_timer_with_fixed_delay("step", _Step(), STEP_EVERY)
            # Per-message state that is updated in place; everything else is in behavior parameters.
            pending: OrderedDict[int, tuple[tuple[float, ...], float]] = OrderedDict()
            minibatch: list[tuple[tuple[float, ...], int]] = []

            def learning(model: Model, stats: Stats, stepping: bool) -> Behavior[LearnerCmd]:
                def on_message(msg: LearnerCmd) -> Behavior[LearnerCmd]:
                    match msg:
                        case Predicted(pid, features, prob):
                            pending[pid] = (features, prob)
                            if len(pending) > PENDING_LIMIT:
                                pending.popitem(last=False)
                        case Feedback(pid, label, reply_to):
                            found = pending.pop(pid, None)
                            if found is None:
                                reply_to.tell(FeedbackResult(False, stats.accuracy, model.version))
                                return Behaviors.same
                            features, prob = found
                            minibatch.append((features, label))
                            new_stats = Stats(stats.feedback + 1, stats.correct + ((prob > 0.5) == bool(label)))
                            reply_to.tell(FeedbackResult(True, new_stats.accuracy, model.version))
                            return learning(model, new_stats, stepping)
                        case _Step() if not stepping and len(minibatch) >= MIN_STEP_SAMPLES:
                            x = np.array([f for f, _ in minibatch])
                            y = np.array([float(label) for _, label in minibatch], dtype=np.float64)
                            minibatch.clear()
                            ctx.pipe_to_self(asyncio.to_thread(sgd_steps, model, x, y), _Stepped)
                            return learning(model, stats, stepping=True)
                        case _Stepped(result):
                            if isinstance(result, Success):
                                predict_ref().tell(UseModel(result.value))
                                return learning(result.value, stats, stepping=False)
                            ctx.log.error("step failed: %r", result.exception)
                            return learning(model, stats, stepping=False)
                        case GetModel(reply_to):
                            reply_to.tell(ModelInfo(model.version, stats.feedback, stats.accuracy, tuple(model.weights.tolist())))
                        case _Step():
                            pass
                    return Behaviors.same

                return Behaviors.receive_message(on_message)

            return learning(untrained(), Stats(), stepping=False)

        return Behaviors.setup(setup)

    return Behaviors.with_timers(with_timers)


# --- guardian: wires predictor and learner, exposes one protocol to HTTP -------------------------


type Cmd = Predict | Feedback | GetModel


def service() -> Behavior[Cmd]:
    def setup(ctx: ActorContext[Cmd]) -> Behavior[Cmd]:
        refs: dict[str, ActorRef[Any]] = {}
        lrn = ctx.spawn(learner(lambda: refs["predictor"]), "learner")  # the two refer to each other
        refs["predictor"] = ctx.spawn(predictor(lrn), "predictor")
        pred: ActorRef[Predict] = refs["predictor"]

        def on_message(msg: Cmd) -> Behavior[Cmd]:
            match msg:
                case Predict():
                    pred.tell(msg)
                case Feedback() | GetModel():
                    lrn.tell(msg)
            return Behaviors.same

        return Behaviors.receive_message(on_message)

    return Behaviors.setup(setup)


# --- HTTP ---------------------------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ANN201
    async with ActorSystem(service(), "online") as system:
        app.state.system = system
        yield


app = FastAPI(title="online learning", lifespan=lifespan)


def ask[R](make: Callable[[ActorRef[R]], Cmd]) -> Any:
    return app.state.system.ask(make, timeout=5)


class Features(BaseModel):
    features: list[float]


class Outcome(BaseModel):
    prediction_id: int
    label: int


@app.post("/predict")
async def predict(body: Features) -> dict[str, Any]:
    p: Prediction = await ask(partial(Predict, tuple(body.features)))
    return {"prediction_id": p.prediction_id, "probability": p.probability, "label": int(p.probability > 0.5), "model_version": p.model_version}


@app.post("/feedback")
async def feedback(body: Outcome) -> dict[str, Any]:
    r: FeedbackResult = await ask(partial(Feedback, body.prediction_id, body.label))
    return {"accepted": r.accepted, "online_accuracy": round(r.online_accuracy, 3), "model_version": r.model_version}


@app.get("/model")
async def model() -> dict[str, Any]:
    m: ModelInfo = await ask(GetModel)
    return {"version": m.version, "feedback_count": m.feedback_count, "online_accuracy": round(m.online_accuracy, 3), "weights": m.weights}


# --- demo client: the world has a hidden rule; the service learns it from feedback alone ------------


def _call(base: str, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method, headers={"content-type": "application/json"})
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())


async def demo(rounds: int = 600) -> None:
    import uvicorn

    server = uvicorn.Server(uvicorn.Config(app, port=8765, log_level="warning"))
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.05)
    base = "http://127.0.0.1:8765"
    rng = np.random.default_rng(0)
    hidden_rule = np.array([1.5, -2.0, 0.5, 1.0])
    try:
        for i in range(1, rounds + 1):
            x = rng.normal(size=N_FEATURES)
            p = await asyncio.to_thread(_call, base, "POST", "/predict", {"features": x.tolist()})
            label = int(x @ hidden_rule + rng.normal(scale=0.5) > 0)  # the outcome the world reveals
            r = await asyncio.to_thread(_call, base, "POST", "/feedback", {"prediction_id": p["prediction_id"], "label": label})
            if i % 100 == 0:
                print(f"round {i:4}  model v{r['model_version']:<3}  online accuracy {r['online_accuracy']:.3f}")
        print(await asyncio.to_thread(_call, base, "GET", "/model"))
    finally:
        server.should_exit = True
        await task


if __name__ == "__main__":
    asyncio.run(demo())
