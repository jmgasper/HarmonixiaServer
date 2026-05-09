-- Local username/password accounts used by API and admin authorization.

CREATE TYPE account_role AS ENUM (
  'admin',
  'user'
);

CREATE TABLE local_accounts (
  id uuid PRIMARY KEY,
  username text NOT NULL,
  password_hash text NOT NULL,
  role account_role NOT NULL DEFAULT 'user',
  disabled boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  CONSTRAINT local_accounts_username_normalized_check
    CHECK (username = btrim(username) AND username <> ''),
  CONSTRAINT local_accounts_password_hash_nonempty_check
    CHECK (password_hash <> '')
);

CREATE UNIQUE INDEX local_accounts_username_ci_idx
  ON local_accounts (lower(username));

CREATE INDEX local_accounts_active_role_idx
  ON local_accounts (role)
  WHERE disabled = false;
