"""contact_kind: add linkedin_people

Revision ID: 0002
Revises: 0001
Create Date: 2026-09-04 00:00:00+00:00

SPEC §6 requires two distinct LinkedIn links per company: the *company page* (published when
the company's own footer links to it, constructed from the domain otherwise) and the
*people-search deep link* the UI renders as "Find people →". SPEC §5's literal ``contact_kind``
list has a member for the first and none for the second, so this revision adds
``linkedin_people``. Overloading ``linkedin_company`` for both would make them
indistinguishable in the panel, which §6's last clause ("the UI must visually distinguish...")
forbids, and would collide on ``uq_contacts_company_id_kind_value`` reasoning.

Review notes — what autogenerate does not produce and was written by hand:

* Alembic autogenerate never emits ``ALTER TYPE ... ADD VALUE``; enum membership changes are
  invisible to it. The whole revision is hand-written.
* ``ADD VALUE IF NOT EXISTS ... AFTER 'linkedin_company'`` keeps the Postgres
  ``enumsortorder`` aligned with the member order of ``db.enums.ContactKind``, which
  ``tests/test_migration.py`` asserts label-for-label. Appending instead would leave the two
  orders disagreeing forever, since a label's sort position cannot be changed later.
* Since Postgres 12 ``ADD VALUE`` runs inside a transaction block, so this needs no
  ``autocommit_block``; the new label is simply not usable until the migration commits, and
  nothing here uses it. (Verified against the Postgres 16 server this repo tests on.)
* Postgres cannot drop a label from an enum, so ``downgrade()`` rebuilds the type. ``contacts``
  is the only table using it (``db/models.py``) and the column has no server default, so the
  rebuild is a rename, a create, one ``ALTER COLUMN ... USING``, and a drop.

Why ``downgrade()`` deletes rows, given SPEC §2/§5's "never delete on refresh": that rule
protects *observed* data — a job that vanished from its source gets ``closed_at`` rather than a
``DELETE``, because the database is the historical record of what a source said. Every
``linkedin_people`` contact is the opposite: ``confidence='constructed'``, a URL derived
deterministically from the company's own name, never fetched and never observed. Deleting one
loses nothing that cannot be recomputed in full by the next ingest run, and there is no way to
keep the row at all once the label it is typed with no longer exists.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0002"
down_revision: str | Sequence[str] | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

NEW_MEMBER = "linkedin_people"
AFTER_MEMBER = "linkedin_company"

# Frozen snapshot of contact_kind *before* this revision — what downgrade() rebuilds.
MEMBERS_AT_0001: tuple[str, ...] = (
    "email",
    "linkedin_company",
    "careers_page",
    "x",
    "github",
    "contact_form",
)


def upgrade() -> None:
    op.execute(
        f"ALTER TYPE contact_kind ADD VALUE IF NOT EXISTS '{NEW_MEMBER}' AFTER '{AFTER_MEMBER}'"
    )


def downgrade() -> None:
    # Constructed, i.e. derived and recomputable — see the module docstring on §2/§5.
    op.execute(f"DELETE FROM contacts WHERE kind = '{NEW_MEMBER}'")
    labels = ", ".join(f"'{member}'" for member in MEMBERS_AT_0001)
    op.execute("ALTER TYPE contact_kind RENAME TO contact_kind_old")
    op.execute(f"CREATE TYPE contact_kind AS ENUM ({labels})")
    op.execute(
        "ALTER TABLE contacts ALTER COLUMN kind TYPE contact_kind USING kind::text::contact_kind"
    )
    op.execute("DROP TYPE contact_kind_old")
