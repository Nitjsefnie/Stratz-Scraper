import time

BATCH_SIZE = 100_000
START_LAST_ID = 0


def main():
    last_id = START_LAST_ID
    total_updated = 0
    processed_rows = 0
    total_rows = 0
    start_time = time.time()
    avg_batch_times = []

    from stratz_scraper.database import connect_pg
    conn = connect_pg(autocommit=False)
    with conn.cursor() as cur:
        # count rows already processed
        cur.execute("SELECT COUNT(*) FROM players WHERE steamAccountId <= %s;", (last_id,))
        processed_rows = cur.fetchone()[0]

        # count total rows in table
        cur.execute("SELECT COUNT(*) FROM players;")
        total_rows = cur.fetchone()[0]

    print(
        f"Starting from steamAccountId <= {last_id}: "
        f"{processed_rows} rows already processed of {total_rows}"
    )

    while True:
        batch_start = time.time()
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH affected AS (
                    SELECT p.steamAccountId
                    FROM players p
                    WHERE p.steamAccountId > %s
                      AND hero_done = TRUE
                    ORDER BY p.steamAccountId
                    LIMIT %s
                ),
                to_reset AS (
                    SELECT a.steamAccountId
                    FROM affected a
                    WHERE NOT EXISTS (
                        SELECT 1 FROM hero_stats h
                        WHERE h.steamAccountId = a.steamAccountId
                    )
                )
                UPDATE players
                SET hero_done = FALSE,
                    hero_refreshed_at = NULL
                FROM to_reset
                WHERE players.steamAccountId = to_reset.steamAccountId
                RETURNING players.steamAccountId;
                """,
                (last_id, BATCH_SIZE),
            )

            rows = cur.fetchall()
            updated = len(rows)
            if updated:
                last_id = rows[-1][0]
            total_updated += updated
            processed_rows += BATCH_SIZE

        batch_time = time.time() - batch_start
        avg_batch_times.append(batch_time)
        avg_time = sum(avg_batch_times) / len(avg_batch_times)
        remaining_rows = max(total_rows - processed_rows, 0)
        eta_seconds = remaining_rows / BATCH_SIZE * avg_time
        eta_h = eta_seconds / 3600

        print(
            f"Batch done: {updated:6d} updated | "
            f"{processed_rows:10d}/{total_rows} processed | "
            f"{batch_time:6.2f}s | ETA: {eta_h:5.2f}h | last_id={last_id}"
        )

        if updated == 0:
            total_elapsed = time.time() - start_time
            print(
                f"All done. Total updated: {total_updated}, "
                f"elapsed {total_elapsed/3600:.2f}h"
            )
            break


if __name__ == "__main__":
    main()
