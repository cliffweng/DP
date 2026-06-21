"""
Module-level task functions – run inside Dask worker threads/processes.
Add new task types by implementing a function and wiring it into _dispatch().
"""
import time
import random


def process_task(task_type: str, params: dict) -> dict:
    """Entry point called by Dask scheduler. Must be a top-level importable function."""
    start = time.perf_counter()
    try:
        result = _dispatch(task_type, params)
        elapsed = time.perf_counter() - start
        return {"success": True, "result": result, "rtt": elapsed}
    except Exception as exc:
        elapsed = time.perf_counter() - start
        return {"success": False, "error": str(exc), "rtt": elapsed}


# ---------------------------------------------------------------------------
# Task registry – add new task types here
# ---------------------------------------------------------------------------

def _dispatch(task_type: str, params: dict) -> dict:
    if task_type == "short_circuit":
        return _short_circuit(params)
    if task_type == "simulate":
        return _simulate(params)
    if task_type == "bond_price":
        return _bond_price(params)
    raise ValueError(f"Unknown task type: {task_type!r}")


def _short_circuit(_params: dict) -> dict:
    """Returns immediately – measures pure Dask scheduling overhead."""
    return {"value": 0, "calc_time": 0.0}


def _simulate(params: dict) -> dict:
    """Sleeps for a random duration in [0, max_calc_time]."""
    max_t = float(params.get("max_calc_time", 2.0))
    sleep_t = random.uniform(0.0, max_t)
    time.sleep(sleep_t)
    return {"calc_time": round(sleep_t, 4)}


def _bond_price(params: dict) -> dict:
    """
    Fixed-coupon bond pricer via discounted cash-flow.
    Compatible with QuantLib pricing conventions (same formula as ql.FixedRateBond).
    """
    face = float(params.get("face_value", 1_000))
    coupon_rate = float(params.get("coupon_rate", 0.05))
    ytm = float(params.get("ytm", random.uniform(0.03, 0.08)))
    periods = int(params.get("periods", 10))
    freq = int(params.get("freq", 1))           # coupons per year
    max_t = float(params.get("max_calc_time", 1.0))

    # Simulate market-data retrieval latency
    time.sleep(random.uniform(0.0, max_t))

    coupon = face * coupon_rate / freq
    n = periods * freq
    r = ytm / freq

    if r == 0:
        price = coupon * n + face
    else:
        pv_coupons = coupon * (1 - (1 + r) ** -n) / r
        pv_face = face / (1 + r) ** n
        price = pv_coupons + pv_face

    # Macaulay duration → modified duration
    duration_num = sum(
        (t / freq) * (coupon / (1 + r) ** t) for t in range(1, n + 1)
    )
    duration_num += (n / freq) * face / (1 + r) ** n
    modified_duration = (duration_num / price) / (1 + r)

    return {
        "price": round(price, 4),
        "ytm": round(ytm, 4),
        "modified_duration": round(modified_duration, 4),
        "coupon_rate": coupon_rate,
        "periods": periods,
    }
