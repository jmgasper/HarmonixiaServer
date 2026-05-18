CREATE TABLE account_track_favorites (
    account_id uuid NOT NULL REFERENCES local_accounts(id) ON DELETE CASCADE,
    track_id uuid NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
    favorited_at timestamptz NOT NULL,
    PRIMARY KEY (account_id, track_id)
);

CREATE INDEX account_track_favorites_account_favorited_idx
    ON account_track_favorites (account_id, favorited_at DESC);
