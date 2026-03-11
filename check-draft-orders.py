import logging
import os
import sys
from typing import List, Optional, Tuple

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
query GetDraftOrders($cursor: String) {
  draftOrders(first: 100, after: $cursor, query: "status:open") {
    edges {
      cursor
      node {
        id
        name
        invoiceUrl
        tags
        lineItems(first: 250) {
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
                  inventoryLevels(first: 50) {
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
        data = shopify_graphql(DRAFTS_QUERY, {"cursor": cursor})
        connection = data["draftOrders"]

        for edge in connection["edges"]:
            drafts.append(edge["node"])

        if not connection["pageInfo"]["hasNextPage"]:
            break

        cursor = connection["pageInfo"]["endCursor"]

    logger.info("Fetched %s open draft orders", len(drafts))
    return drafts


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


def evaluate_draft(draft: dict) -> Tuple[bool, List[str]]:
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
            reasons.append(f"Variant has no inventory item: {variant.get('displayName', 'unknown')}")
            continue

        tracked = bool(inventory_item.get("tracked"))
        sku = inventory_item.get("sku") or "(no sku)"

        if not tracked:
            reasons.append(f"Ignoring untracked item {sku}")
            continue

        tracked_line_count += 1

        levels = inventory_item.get("inventoryLevels", {}).get("edges", [])
        available = available_at_location(levels, SHOPIFY_LOCATION_ID)

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

    for draft in drafts:
        name = draft["name"]
        draft_id = draft["id"]
        tags = normalize_tags(draft.get("tags", []))
        invoice_url = draft.get("invoiceUrl")

        if has_excluded_tag(tags):
            logger.info("Skipping %s because it has an excluded tag", name)
            continue

        # Safety rule:
        # updating a draft can interfere with a started invoice checkout,
        # so skip drafts that already have an invoice URL.
        if invoice_url:
            logger.info("Skipping %s because invoiceUrl exists", name)
            continue

        is_ready, reasons = evaluate_draft(draft)
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
