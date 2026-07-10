# RR Click Drag Separation Design

## Goal

Split RR chart interaction into two explicit behaviors:

- Single click selects an RR item only.
- Dragging a handle changes RR prices only after a clear pointer move threshold.

This change is limited to RR overlays in the Qt Kline analysis window.

## Current Problem

The current RR interaction arms a drag on pointer press and applies the update on pointer release. As a result, a simple click near `entry / stop / take-profit` can change the RR prices even when the user intended only to select the RR block.

## Approved Behavior

1. Single click on RR handle area selects the RR item and does not change any price.
2. Double click still opens the RR parameter card.
3. RR price changes are allowed only after pointer movement exceeds a small pixel threshold.
4. Locked RR items remain selectable and viewable, but are not draggable.
5. Line drawing and non-RR tools are out of scope for this change.

## Design

### Event Model

- On pointer press over an RR handle, record a pending RR drag state.
- Do not mutate RR data on press.
- On pointer move, compare current scene position with the press scene position.
- Only when the movement exceeds a fixed pixel threshold does the pending RR drag become active.
- Once active, RR drag updates continue to use the existing RR price recalculation logic.
- On pointer release:
  - if the drag never became active, clear the pending state without saving any RR change;
  - if the drag became active, save the final RR payload and re-render.

### State

The RR drag state will track:

- selected RR index
- drag mode (`rr_entry`, `rr_stop`, `rr_tp`)
- press scene position
- whether drag activation threshold has been crossed

### Testing

Add regression coverage for:

1. clicking an RR handle without move does not change RR values
2. dragging an RR handle with sufficient pointer movement still updates RR values
3. existing alert snapshot preservation behavior remains intact

## Risks

- The threshold must use scene pixels, not chart price/time values.
- The change must not alter double-click behavior.
- The change must not silently broaden to non-RR line interactions in this pass.
