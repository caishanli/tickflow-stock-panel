# Design: Enforce Minimum Commission and Separate Buy/Sell Fee Rates

## Overview
This design addresses trade signal divergence between jqengine and JoinQuant by enforcing minimum commission and supporting separate buy/sell fee rates in the order execution engine.

## Problem
1. The `order()` function computes fees as `turnover * fee` without enforcing the `min_commission=5` yuan minimum specified in the strategy's `set_order_cost`
2. The engine only stores `open_commission` as a flat rate, ignoring separate buy/sell commission rates

## Solution
Three changes to `backend/app/quant/jqengine/engine/jq/api.py`:

### 1. Update `set_order_cost` to store full cost config
- Store all OrderCost fields in `_state["fee_config"]` dictionary
- Maintain backward compatibility with `_state["fee"]`
- Support both OrderCost objects and dictionaries

### 2. Update fee calculation in `order()` to enforce min_commission
- Use separate `open_commission` and `close_commission` rates based on trade direction
- Enforce `min_commission` as minimum fee amount using `max(turnover * comm_rate, min_comm)`
- Maintain backward compatibility when `fee_config` is not set

### 3. Initialize `fee_config=None` in `_reset`
- Ensure `fee_config` is properly initialized to `None` during state reset
- Prevent stale fee configuration from previous backtests

## Implementation Details
The implementation follows the exact specification provided in the task brief:
- Store full fee configuration in `_state["fee_config"]` dictionary
- Use conditional logic to select appropriate commission rate based on trade direction
- Apply minimum commission enforcement with `max()` function
- Maintain backward compatibility for existing code

## Testing
- Run existing test suite: `cd backend && uv run --extra dev pytest tests/ -x -q`
- Key test file: `tests/quant/test_engine_order_rules.py` - all 14 tests pass
- Additional commission-related tests pass

## Trade-offs
- **Backward Compatibility**: Maintained by keeping `_state["fee"]` and defaulting to `fee` when `fee_config` is not set
- **Performance**: Minimal overhead from dictionary lookup and conditional logic
- **Complexity**: Small increase in code complexity for proper fee handling

## Future Considerations
- Could extend to support `close_today_commission` for day trading
- Could add validation for fee configuration values
- Could add logging for fee calculations for debugging