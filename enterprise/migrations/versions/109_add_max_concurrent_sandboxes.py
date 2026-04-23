"""Add max_concurrent_sandboxes to org and org_member tables.

Adds per-org default and per-user override for concurrent sandbox limits.
- org.max_concurrent_sandboxes: org-wide default (default 3 for personal workspaces)
- org_member.max_concurrent_sandboxes_override: per-user override (NULL = use org default)

Also sets OpenHands org to have a limit of 10.

Revision ID: 109
Revises: 108
Create Date: 2026-04-22
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '109'
down_revision: Union[str, None] = '108'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add max_concurrent_sandboxes to org table with default of 3
    op.add_column(
        'org',
        sa.Column(
            'max_concurrent_sandboxes',
            sa.Integer(),
            nullable=False,
            server_default='3',
        ),
    )

    # Add max_concurrent_sandboxes_override to org_member table (NULL = use org default)
    op.add_column(
        'org_member',
        sa.Column(
            'max_concurrent_sandboxes_override',
            sa.Integer(),
            nullable=True,
        ),
    )

    # Set OpenHands org to have a limit of 10
    # Using raw SQL to update by name since we don't have the org ID
    op.execute("UPDATE org SET max_concurrent_sandboxes = 10 WHERE name = 'OpenHands'")


def downgrade() -> None:
    op.drop_column('org_member', 'max_concurrent_sandboxes_override')
    op.drop_column('org', 'max_concurrent_sandboxes')
