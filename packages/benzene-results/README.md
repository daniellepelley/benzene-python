# benzene-results

The bottom layer of the [Benzene Python port](https://github.com/daniellepelley/benzene-python):
the `Result` type and the status vocabulary. **Zero dependencies.**

A domain handler returns a value, never raising for an expected outcome:

```python
from benzene.results import Result

def create_order(request) -> Result:
    if not request.get("sku"):
        return Result.bad_request("sku is required")
    return Result.created({"id": "ord_123"})
```

Install just this layer if all you want is Benzene's result/status model (e.g. to type a domain
service's return values) without any pipeline or transport code:

```bash
pip install benzene-results
```

Provides `Result`, `Status` (the wire-contract status constants), `is_successful`, and the
`SUCCESS_STATUSES` / `FAILURE_STATUSES` / `KNOWN_STATUSES` sets. It contributes the `benzene.results`
subpackage to the shared `benzene` namespace. Mirrors .NET's `Benzene.Results` plus the status
vocabulary from `Benzene.Abstractions`.
