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


class _JobSupabase:
    def __init__(self, existing_history=None):
        self.existing_history = existing_history
        self.created_history_records = []

    def get_dataset_with_items(self, dataset_id: int):
        return {
            "id": dataset_id,
            "user_id": "owner-1",
            "dataset_items": [{"id": 999}],
        }

    def has_platform_dataset_admin(self, user_id: str) -> bool:
        return False

    def has_dataset_user_access(
        self,
        user_id: str,
        dataset_id: int,
        required_permission: str = "read",
    ) -> bool:
        return False

    def get_galaxy_history_by_dataset(self, dataset_id: int):
        return self.existing_history

    def get_or_create_galaxy_history(
        self,
        *,
        dataset_id: int,
        history_id: str,
        history_name: str,
        s3_base_path: str,
    ):
        record = {
            "id": 321,
            "dataset_id": dataset_id,
            "history_id": history_id,
            "history_name": history_name,
            "s3_base_path": s3_base_path,
        }
        self.created_history_records.append(record)
        return record


class _JobGalaxy:
    def __init__(self, default_object_store_id=None):
        self.config = type(
            "Config",
            (),
            {
                "default_object_store_id": default_object_store_id,
                "default_intermediate_object_store_id": None,
                "default_outputs_object_store_id": None,
            },
        )()
        self.create_history_calls = []
        self.set_history_preferred_object_store_calls = []

    def create_history(self, name: str, preferred_object_store_id: str | None = None):
        self.create_history_calls.append((name, preferred_object_store_id))
        return type("History", (), {"id": "history-new", "name": name})()

    def set_history_preferred_object_store(
        self, history_id: str, preferred_object_store_id: str
    ) -> None:
        self.set_history_preferred_object_store_calls.append(
            (history_id, preferred_object_store_id)
        )


def test_create_job_sets_scratch_store_on_new_galaxy_eu_history(monkeypatch):
    supabase = _JobSupabase(existing_history=None)
    galaxy = _JobGalaxy(default_object_store_id=None)
    invocation_kwargs = {}

    monkeypatch.setattr(
        service,
        "build_workflow_parameters",
        lambda **kwargs: {"1": {"export_path": "42/"}},
    )
    monkeypatch.setattr(
        service,
        "invoke_workflow_with_collection",
        lambda **kwargs: invocation_kwargs.update(kwargs) or {"invocation_id": "inv-1"},
    )

    result = service.create_job(
        dataset_id="42",
        workflow_name="EndToEndPipeline-GalaxyEU",
        overwrite=False,
        parameters={},
        requesting_user_id="owner-1",
        requesting_user_email="owner@example.com",
        galaxy=galaxy,
        supabase=supabase,
        storage=object(),
    )

    assert result == {"invocation_id": "inv-1"}
    assert galaxy.create_history_calls == [
        ("EndToEndPipeline-GalaxyEU - Dataset 42", "s3_scratch_netapp01")
    ]
    assert galaxy.set_history_preferred_object_store_calls == []
    assert invocation_kwargs["preferred_object_store_id"] == "s3_scratch_netapp01"
    assert invocation_kwargs["history_id"] == "history-new"


def test_create_job_heals_existing_history_to_configured_store(monkeypatch):
    supabase = _JobSupabase(
        existing_history={
            "id": 77,
            "history_id": "history-existing",
            "s3_base_path": "42/",
        }
    )
    galaxy = _JobGalaxy(default_object_store_id="configured-scratch")
    invocation_kwargs = {}

    monkeypatch.setattr(
        service,
        "build_workflow_parameters",
        lambda **kwargs: {"1": {"export_path": "42/"}},
    )
    monkeypatch.setattr(
        service,
        "invoke_workflow_with_collection",
        lambda **kwargs: invocation_kwargs.update(kwargs) or {"invocation_id": "inv-2"},
    )

    result = service.create_job(
        dataset_id="42",
        workflow_name="EndToEndPipeline-GalaxyEU",
        overwrite=False,
        parameters={},
        requesting_user_id="owner-1",
        requesting_user_email="owner@example.com",
        galaxy=galaxy,
        supabase=supabase,
        storage=object(),
    )

    assert result == {"invocation_id": "inv-2"}
    assert galaxy.create_history_calls == []
    assert galaxy.set_history_preferred_object_store_calls == [
        ("history-existing", "configured-scratch")
    ]
    assert invocation_kwargs["preferred_object_store_id"] == "configured-scratch"
    assert invocation_kwargs["history_id"] == "history-existing"
