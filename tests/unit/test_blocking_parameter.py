#!/usr/bin/env python3
"""
Test that the blocking parameter is correctly set to True by default.
"""

import pytest
from dagster import AssetKey
from dg_sqlmesh import SQLMeshResource
from dg_sqlmesh.sqlmesh_asset_check_utils import create_asset_checks_from_model


class TestBlockingParameter:
    """Test that audit checks are created with blocking=True by default."""

    def test_blocking_parameter_default(
        self, sqlmesh_resource: SQLMeshResource
    ) -> None:
        """Test that the blocking parameter is correctly set to True by default."""

        # Get the stg_supplies model which has 3 built-in audits
        context = sqlmesh_resource.context
        model = context.get_model("sqlmesh_jaffle_platform.stg_supplies")

        print(f"Model: {model.name}")
        print(f"Audits: {model.audits}")

        # Create asset checks from the model
        asset_key = AssetKey(["jaffle_test", "sqlmesh_jaffle_platform", "stg_supplies"])
        check_specs = create_asset_checks_from_model(model, asset_key)

        print(f"\nCreated {len(check_specs)} check specs:")
        for i, check_spec in enumerate(check_specs):
            print(f"  Check {i + 1}: {check_spec.name}")
            print(f"    Blocking: {check_spec.blocking}")
            print(f"    Metadata: {check_spec.metadata}")

            # Verify that blocking is False (we let SQLMesh handle blocking)
            assert check_spec.blocking is False, (
                f"Check {check_spec.name} should be blocking=False"
            )

            # Verify that metadata includes audit_blocking (except for sqlmesh_execution_status)
            if check_spec.name != "sqlmesh_execution_status":
                assert "audit_blocking" in check_spec.metadata, (
                    f"Check {check_spec.name} should have audit_blocking in metadata"
                )
                # Blocking should be False for explicit non-blocking audits, True otherwise
                if check_spec.name.endswith("_non_blocking"):
                    assert check_spec.metadata["audit_blocking"] is False, (
                        f"Check {check_spec.name} should have audit_blocking=False for non-blocking variant"
                    )
                else:
                    assert check_spec.metadata["audit_blocking"] is True, (
                        f"Check {check_spec.name} should have audit_blocking=True"
                    )

        print("\n✅ All checks are correctly set to blocking=False!")

        # Verify we have the expected audits (including the non-blocking variant and sqlmesh_execution_status)
        expected_audits = {
            "number_of_rows",
            "not_null",
            "not_constant_non_blocking",
            "sqlmesh_execution_status",
        }
        actual_audits = {check_spec.name for check_spec in check_specs}
        assert actual_audits == expected_audits, (
            f"Expected audits {expected_audits}, got {actual_audits}"
        )

    def test_blocking_parameter_with_custom_audit(
        self, sqlmesh_resource: SQLMeshResource
    ) -> None:
        """Test that custom audits also respect the blocking parameter."""

        # Get a model with custom audits if available
        context = sqlmesh_resource.context
        models = context.snapshots.values()

        # Find a model with audits
        model_with_audits = None
        for snapshot in models:
            if (
                hasattr(snapshot, "node")
                and hasattr(snapshot.node, "audits")
                and snapshot.node.audits
            ):
                model_with_audits = snapshot.node
                break

        if model_with_audits:
            print(f"Testing model: {model_with_audits.name}")

            # Create asset checks from the model
            asset_key = AssetKey(
                [
                    "jaffle_test",
                    "sqlmesh_jaffle_platform",
                    model_with_audits.name.split(".")[-1],
                ]
            )
            check_specs = create_asset_checks_from_model(model_with_audits, asset_key)

            for check_spec in check_specs:
                print(f"  Check: {check_spec.name} - Blocking: {check_spec.blocking}")
                # All audits should be non-blocking (we let SQLMesh handle blocking)
                assert check_spec.blocking is False, (
                    f"Check {check_spec.name} should be blocking=False"
                )

            print("✅ Custom audits also respect blocking=False!")
        else:
            pytest.skip("No models with audits found for testing")
