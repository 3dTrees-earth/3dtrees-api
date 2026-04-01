from trees_api.routes.jobs import service


class _FakeSupabase:
    def __init__(self, *, platform_admin: bool = False, dataset_access: bool = False):
        self.platform_admin = platform_admin
        self.dataset_access = dataset_access

    def has_platform_dataset_admin(self, user_id: str) -> bool:
        return self.platform_admin

    def has_dataset_user_access(
        self,
        user_id: str,
        dataset_id: int,
        required_permission: str = "read",
    ) -> bool:
        return self.dataset_access


def test_can_access_dataset_jobs_allows_processor_by_configured_user_id(monkeypatch):
    monkeypatch.setenv("SUPABASE_PROCESSOR_USER_ID", "processor-user-id")

    allowed = service._can_access_dataset_jobs(
        dataset={"user_id": "owner-1"},
        dataset_id=10,
        requesting_user_id="processor-user-id",
        requesting_user_email="not-processor@example.com",
        required_permission="edit",
        supabase=_FakeSupabase(),
    )

    assert allowed is True


def test_can_access_dataset_jobs_keeps_legacy_processor_email_fallback(monkeypatch):
    monkeypatch.delenv("SUPABASE_PROCESSOR_USER_ID", raising=False)

    allowed = service._can_access_dataset_jobs(
        dataset={"user_id": "owner-1"},
        dataset_id=10,
        requesting_user_id="some-other-user",
        requesting_user_email="processor@3dtrees.earth",
        required_permission="edit",
        supabase=_FakeSupabase(),
    )

    assert allowed is True


def test_can_access_dataset_jobs_denies_unshared_foreign_user(monkeypatch):
    monkeypatch.delenv("SUPABASE_PROCESSOR_USER_ID", raising=False)

    allowed = service._can_access_dataset_jobs(
        dataset={"user_id": "owner-1"},
        dataset_id=10,
        requesting_user_id="outsider-1",
        requesting_user_email="outsider@example.com",
        required_permission="edit",
        supabase=_FakeSupabase(platform_admin=False, dataset_access=False),
    )

    assert allowed is False
