-- Singular test: returns rows that FAIL the assertion.
-- An empty result means the test passes.
-- Flags any feat_technical row whose timestamp is in the future,
-- which would indicate a clock-sync bug or bad data.
select symbol, timestamp
from {{ ref('feat_technical') }}
where timestamp > current_timestamp
