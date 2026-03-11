import logging
import os
import sys
from typing import Dict, List, Optional, Set, Tuple

import requests
from dotenv import load_dotenv

load_dotenv()

SHOPIFY_SHOP = os.getenv("SHOPIFY_SHOP", "").strip()
SHOPIFY_TOKEN = os.getenv("SHOPIFY_TOKEN", "").strip()
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-10").strip()
SHOPIFY_LOCATION_ID = os.getenv("SHOPIFY_LOCATION_ID", "").strip()
READY_TAG = os.getenv("READY_TAG", "instock-ready").strip()
EXCLUDE_TAGS = {
    t.strip()
    for t in os.getenv(
        "EXCLUDE_TAGS",
        "split-backorder-done,split-backorder-child,split-backorder-processing",
    ).split(",")
    if t.strip()
}
DRY_RUN = os.getenv("DRY_RUN", "true").strip().lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper().strip()

DRAFTS_PAGE_SIZE = 25
LINE_ITEMS_PAGE_SIZE = 100
INVENTORY_BATCH_SIZE = 25
INVENTORY_LEVELS_PAGE_SIZE = 20

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

if not SHOPIFY_SHOP or not SHOPIFY_TOKEN or not SHOPIFY_LOCATION_ID:
    logger.error("Missing required environment variables.")
    sys.exit(1)

GRAPHQL_URL = f"https://{SHOPIFY_SHOP}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json",
}

DRAFTS_QUERY = """
query GetDraftOrders($cursor: String, $pageSize: Int!, $lineItemsPageSize: Int!) {
  draftOrders(first: $pageSize, after: $cursor, query: "status:open") {
    edges {
      cursor
      node {
        id
        name
        invoiceUrl
        tags
        lineItems(first: $lineItemsPageSize) {
          edges {
            node {
              id
              title
              quantity
              variant {
                id
                displayName
                inventoryItem {
                  id
                  tracked
                  sku
                }
              }
            }
          }
          pageInfo {
            hasNextPage
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

INVENTORY_ITEMS_QUERY = """
query GetInventoryItems($ids: [ID!]!, $levelsPageSize: Int!) {
  nodes(ids: $ids) {
    ... on InventoryItem {
      id
      tracked
      sku
      inventoryLevels(first: $levelsPageSize) {
        edges {
          node {
            location {
              id
              name
            }
            quantities(names: ["available"]) {
              name
              quantity
            }
          }
        }
      }
    }
  }
}
"""

DRAFT_UPDATE_MUTATION = """
mutation UpdateDraftTags($id: ID!, $input: DraftOrderInput!) {
  draftOrderUpdate(id: $id, input: $input) {
    draftOrder {
      id
      name
      tags
    }
    userErrors {
      field
      message
    }
  }
}
"""


def chunked(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def shopify_graphql(query: str, variables: Optional[dict] = None) -> dict:
    response = requests.post(
        GRAPHQL_URL,
        headers=HEADERS,
        json={"query": query, "variables": variables or {}},
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()

    if "errors" in payload:
        raise RuntimeError(f"GraphQL errors: {payload['errors']}")

    return payload["data"]


def normalize_tags(tags: List[str]) -> List[str]:
    return sorted({tag.strip() for tag in tags if tag and tag.strip()})


def has_excluded_tag(tags: List[str]) -> bool:
    return any(tag in EXCLUDE_TAGS for tag in tags)


def fetch_open_drafts() -> List[dict]:
    drafts: List[dict] = []
    cursor = None

    while True:
        data = shopify_graphql(
            DRAFTS_QUERY,
            {
                "cursor": cursor,
                "pageSize": DRAFTS_PAGE_SIZE,
                "lineItemsPageSize": LINE_ITEMS_PAGE_SIZE,
            },
        )
        connection = data["draftOrders"]

        for edge in connection["edges"]:
            node = edge["node"]
            if node["lineItems"]["pageInfo"]["hasNextPage"]:
                logger.warning(
                    "Draft %s has more than %s line items; extra lines are not being evaluated",
                    node["name"],
                    LINE_ITEMS_PAGE_SIZE,
                )
            drafts.append(node)

        if not connection["pageInfo"]["hasNextPage"]:
            break

        cursor = connection["pageInfo"]["endCursor"]

    logger.info("Fetched %s open draft orders", len(drafts))
    return drafts


def collect_inventory_item_ids(drafts: List[dict]) -> List[str]:
    ids: Set[str] = set()

    for draft in drafts:
        tags = normalize_tags(draft.get("tags", []))
        if has_excluded_tag(tags):
            continue

        for edge in draft["lineItems"]["edges"]:
            line = edge["node"]
            variant = line.get("variant")
            if not variant:
                continue

            inventory_item = variant.get("inventoryItem")
            if not inventory_item:
                continue

            inventory_item_id = inventory_item.get("id")
            tracked = bool(inventory_item.get("tracked"))

            if inventory_item_id and tracked:
                ids.add(inventory_item_id)

    return sorted(ids)


def available_at_location(inventory_levels_edges: List[dict], location_id: str) -> Optional[int]:
    for edge in inventory_levels_edges:
        node = edge["node"]
        location = node.get("location")

        if location and location.get("id") == location_id:
            for qty in node.get("quantities", []):
                if qty.get("name") == "available":
                    return int(qty.get("quantity", 0))
            return 0

    return None


def fetch_inventory_availability(inventory_item_ids: List[str]) -> Dict[str, Dict[str, Optional[int]]]:
    results: Dict[str, Dict[str, Optional[int]]] = {}

    if not inventory_item_ids:
        return results

    batches = chunked(inventory_item_ids, INVENTORY_BATCH_SIZE)

    for batch_num, batch_ids in enumerate(batches, start=1):
        data = shopify_graphql(
            INVENTORY_ITEMS_QUERY,
            {
                "ids": batch_ids,
                "levelsPageSize": INVENTORY_LEVELS_PAGE_SIZE,
            },
        )

        nodes = data.get("nodes", [])
        logger.info(
            "Fetched inventory batch %s/%s (%s inventory items)",
            batch_num,
            len(batches),
            len(batch_ids),
        )

        for node in nodes:
            if not node:
                continue

            inventory_item_id = node.get("id")
            if not inventory_item_id:
                continue

            levels = node.get("inventoryLevels", {}).get("edges", [])
            results[inventory_item_id] = {
                "sku": node.get("sku") or "(no sku)",
                "available": available_at_location(levels, SHOPIFY_LOCATION_ID),
            }

    return results


def evaluate_draft(draft: dict, availability_map: Dict[str, Dict[str, Optional[int]]]) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    tracked_line_count = 0

    for edge in draft["lineItems"]["edges"]:
        line = edge["node"]
        qty_needed = int(line["quantity"])
        variant = line.get("variant")

        if not variant:
            reasons.append(f"Ignoring custom line '{line['title']}'")
            continue

        inventory_item = variant.get("inventoryItem")
        if not inventory_item:
            reasons.append(
                f"Variant has no inventory item: {variant.get('displayName', 'unknown')}"
            )
            continue

        tracked = bool(inventory_item.get("tracked"))
        sku = inventory_item.get("sku") or "(no sku)"
        inventory_item_id = inventory_item.get("id")

        if not tracked:
            reasons.append(f"Ignoring untracked item {sku}")
            continue

        tracked_line_count += 1

        if not inventory_item_id:
            reasons.append(f"Missing inventory item ID for {sku}")
            return False, reasons

        availability_info = availability_map.get(inventory_item_id)
        if not availability_info:
            reasons.append(f"No inventory lookup result for {sku}")
            return False, reasons

        available = availability_info.get("available")

        if available is None:
            reasons.append(f"No inventory level at target location for {sku}")
            return False, reasons

        if available < qty_needed:
            reasons.append(f"Insufficient inventory for {sku}: need {qty_needed}, have {available}")
            return False, reasons

    if tracked_line_count == 0:
        reasons.append("No tracked variant lines found")
        return False, reasons

    return True, reasons


def update_draft_tags(draft_id: str, current_tags: List[str], should_have_ready_tag: bool) -> None:
    new_tags = set(normalize_tags(current_tags))

    if should_have_ready_tag:
        new_tags.add(READY_TAG)
    else:
        new_tags.discard(READY_TAG)

    final_tags = sorted(new_tags)

    if final_tags == normalize_tags(current_tags):
        return

    logger.info("Updating tags for %s -> %s", draft_id, final_tags)

    if DRY_RUN:
        logger.info("DRY RUN enabled; skipping tag update")
        return

    data = shopify_graphql(
        DRAFT_UPDATE_MUTATION,
        {
            "id": draft_id,
            "input": {
                "tags": final_tags,
            },
        },
    )

    user_errors = data["draftOrderUpdate"].get("userErrors", [])
    if user_errors:
        raise RuntimeError(f"draftOrderUpdate userErrors: {user_errors}")


def main() -> None:
    drafts = fetch_open_drafts()
    inventory_item_ids = collect_inventory_item_ids(drafts)

    logger.info("Need inventory checks for %s unique tracked inventory items", len(inventory_item_ids))

    availability_map = fetch_inventory_availability(inventory_item_ids)

    for draft in drafts:
        name = draft["name"]
        draft_id = draft["id"]
        tags = normalize_tags(draft.get("tags", []))

        if has_excluded_tag(tags):
            logger.info("Skipping %s because it has an excluded tag", name)
            continue

        is_ready, reasons = evaluate_draft(draft, availability_map)
        has_ready_tag = READY_TAG in tags

        logger.info(
            "Draft %s | ready=%s | has_tag=%s | reasons=%s",
            name,
            is_ready,
            has_ready_tag,
            reasons,
        )

        if is_ready and not has_ready_tag:
            update_draft_tags(draft_id, tags, True)
            logger.info("Added %s to %s", READY_TAG, name)
        elif not is_ready and has_ready_tag:
            update_draft_tags(draft_id, tags, False)
            logger.info("Removed %s from %s", READY_TAG, name)
        else:
            logger.info("No tag change needed for %s", name)


if __name__ == "__main__":
    main()
