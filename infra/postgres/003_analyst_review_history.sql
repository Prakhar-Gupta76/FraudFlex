ALTER TABLE analyst_reviews
    DROP CONSTRAINT IF EXISTS analyst_reviews_alert_id_key;

ALTER TABLE analyst_reviews
    DROP CONSTRAINT IF EXISTS analyst_reviews_outcome_check;

ALTER TABLE analyst_reviews
    ADD COLUMN IF NOT EXISTS previous_status VARCHAR(16),
    ADD COLUMN IF NOT EXISTS new_status VARCHAR(16);

UPDATE analyst_reviews
SET outcome = 'needs_further_investigation'
WHERE outcome = 'needs_more_information';

UPDATE analyst_reviews
SET previous_status = COALESCE(previous_status, 'open'),
    new_status = COALESCE(new_status, 'resolved');

ALTER TABLE analyst_reviews
    ALTER COLUMN previous_status SET NOT NULL,
    ALTER COLUMN new_status SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'analyst_reviews_outcome_check'
    ) THEN
        ALTER TABLE analyst_reviews
            ADD CONSTRAINT analyst_reviews_outcome_check
            CHECK (
                outcome IN (
                    'confirmed_fraud',
                    'legitimate',
                    'needs_further_investigation'
                )
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'analyst_reviews_status_transition_check'
    ) THEN
        ALTER TABLE analyst_reviews
            ADD CONSTRAINT analyst_reviews_status_transition_check
            CHECK (
                previous_status IN ('open', 'assigned')
                AND new_status IN ('open', 'assigned', 'resolved')
            );
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS analyst_reviews_alert_time_idx
    ON analyst_reviews (alert_id, reviewed_at DESC, review_id);
