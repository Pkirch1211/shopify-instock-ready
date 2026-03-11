import logging
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
            logger.info("Skipping %s بسبب excluded tag", name)
            continue

        # Safety: avoid updating drafts that already have an invoice URL,
        # since updating a draft can unlink an in-progress checkout.
        if invoice_url:
            logger.info("Skipping %s because invoiceUrl exists", name)
            continue

        is_ready, reasons = evaluate_draft(draft)
        has_ready_tag = READY_TAG in tags

        logger.info("Draft %s | ready=%s | has_tag=%s | reasons=%s", name, is_ready, has_ready_tag, reasons)

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
