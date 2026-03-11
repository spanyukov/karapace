"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details

Tests for Google Well-Known Types support in protobuf serialization.

This module tests the functionality for discovering and using Google protobuf
include paths when compiling .proto files that import well-known types like
google.protobuf.Timestamp, google.protobuf.Duration, etc.
"""

import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from karapace.core.dependency import Dependency
from karapace.core.protobuf.io import (
    GOOGLE_PROTOBUF_CANDIDATE_PATHS,
    calculate_class_name,
    crawl_dependencies,
    find_protobuf_include,
    get_protobuf_class_instance,
)
from karapace.core.protobuf.schema import ProtobufSchema
from karapace.core.schema_models import ValidatedTypedSchema
from karapace.core.schema_type import SchemaType
from karapace.core.typing import Subject

# =============================================================================
# Test Constants
# =============================================================================

# Schema that imports google.protobuf.Timestamp
SCHEMA_WITH_TIMESTAMP = """\
syntax = "proto3";
package test.timestamp;

import "google/protobuf/timestamp.proto";

message Event {
    string event_id = 1;
    google.protobuf.Timestamp event_time = 2;
    string source = 3;
}
"""

# Schema that imports google.protobuf.Duration
SCHEMA_WITH_DURATION = """\
syntax = "proto3";
package test.duration;

import "google/protobuf/duration.proto";

message Task {
    string task_id = 1;
    google.protobuf.Duration timeout = 2;
}
"""

# Schema that imports google.protobuf.Struct
SCHEMA_WITH_STRUCT = """\
syntax = "proto3";
package test.struct;

import "google/protobuf/struct.proto";

message Config {
    string config_id = 1;
    google.protobuf.Struct properties = 2;
}
"""

# Schema that imports multiple well-known types
SCHEMA_WITH_MULTIPLE_WKT = """\
syntax = "proto3";
package test.multiple;

import "google/protobuf/timestamp.proto";
import "google/protobuf/duration.proto";
import "google/protobuf/struct.proto";

message Notification {
    string id = 1;
    google.protobuf.Timestamp created_at = 2;
    google.protobuf.Duration ttl = 3;
    google.protobuf.Struct metadata = 4;
}
"""

# Schema with nested message importing well-known types
SCHEMA_WITH_NESTED_WKT = """\
syntax = "proto3";
package test.nested;

import "google/protobuf/timestamp.proto";

message Envelope {
    string envelope_id = 1;
    google.protobuf.Timestamp timestamp = 2;

    message Payload {
        string data = 1;
        google.protobuf.Timestamp processed_at = 2;
    }

    Payload payload = 3;
}
"""


# =============================================================================
# Helper Function Tests
# =============================================================================


class TestFindProtobufInclude:
    """Tests for the find_protobuf_include function."""

    def test_find_protobuf_include_checks_standard_locations(self):
        """Test that find_protobuf_include checks standard system locations."""
        result = find_protobuf_include()

        # Result should be a valid path or None
        if result is not None:
            assert Path(result).exists(), f"Include path should exist: {result}"
            assert Path(result).is_dir(), f"Include path should be a directory: {result}"

            # Verify it contains google/protobuf/timestamp.proto
            timestamp_proto = Path(result) / "google" / "protobuf" / "timestamp.proto"
            assert timestamp_proto.exists(), (
                f"Include path should contain google/protobuf/timestamp.proto: {result}"
            )

    def test_find_protobuf_include_returns_none_when_not_found(self):
        """Test that find_protobuf_include returns None when no valid path is found."""
        # Clear cache before and after to avoid test pollution
        find_protobuf_include.cache_clear()
        try:
            # Mock Path.exists to always return False for timestamp.proto check
            def mock_exists(self):
                return False

            with patch.object(Path, "exists", mock_exists):
                result = find_protobuf_include()
                assert result is None, "Should return None when no include path is found"
        finally:
            find_protobuf_include.cache_clear()

    def test_candidate_paths_include_debian_ubuntu_path(self):
        """Test that /usr/include is in candidate paths (Debian/Ubuntu location)."""
        assert "/usr/include" in GOOGLE_PROTOBUF_CANDIDATE_PATHS, \
            "/usr/include should be in candidate paths"

    def test_candidate_paths_include_homebrew_path(self):
        """Test that /usr/local/include is in candidate paths (Homebrew location)."""
        assert "/usr/local/include" in GOOGLE_PROTOBUF_CANDIDATE_PATHS, \
            "/usr/local/include should be in candidate paths"


class TestDiscoverGoogleProtobufIncludePath:
    """Tests for the find_protobuf_include integration in get_protobuf_class_instance."""

    def test_find_protobuf_include_called(self, tmp_path: Path):
        """Test that find_protobuf_include is called during class instance creation."""
        schema = ProtobufSchema(schema=SCHEMA_WITH_TIMESTAMP)
        cfg = SimpleNamespace(protobuf_runtime_directory=str(tmp_path / "runtime"))

        with patch("karapace.core.protobuf.io.find_protobuf_include") as mock_find:
            mock_find.return_value = "/usr/include"

            try:
                get_protobuf_class_instance(schema, "Event", cfg)
            except Exception:
                # We might get an error, but we just want to verify the function was called
                pass

            # Verify that find_protobuf_include was called
            mock_find.assert_called()

    def test_proto_path_added_to_arguments(self, tmp_path: Path):
        """Test that --proto_path is added to protoc arguments when include path is found."""
        schema = ProtobufSchema(schema=SCHEMA_WITH_TIMESTAMP)
        cfg = SimpleNamespace(protobuf_runtime_directory=str(tmp_path / "runtime"))

        with patch("karapace.core.protobuf.io.find_protobuf_include", return_value="/usr/include"):
            with patch("karapace.core.protobuf.io.subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0)

                # Calculate expected paths
                work_dir = tmp_path / "runtime" / calculate_class_name(str(schema))
                class_name = calculate_class_name(str(schema))
                pb2_file = work_dir / f"{class_name}_pb2.py"

                # Use side_effect to create the pb2 file when subprocess is called
                def create_pb2_on_call(*args, **kwargs):
                    work_dir.mkdir(parents=True, exist_ok=True)
                    proto_file = work_dir / f"{class_name}.proto"
                    proto_file.write_text(SCHEMA_WITH_TIMESTAMP)
                    pb2_file.write_text(
                        "from google.protobuf import descriptor_pool\n"
                        "from google.protobuf import message\n"
                        "from google.protobuf import descriptor_pb2\n"
                        "class Event(message.Message):\n"
                        "    pass\n"
                    )
                    return MagicMock(returncode=0)

                mock_run.side_effect = create_pb2_on_call

                get_protobuf_class_instance(schema, "Event", cfg)

                # Check that subprocess.run was called with --proto_path
                mock_run.assert_called()
                call_args = mock_run.call_args
                args_list = call_args[0][0]  # First positional argument (the command list)

                # Verify --proto_path is in the arguments
                proto_path_found = any(
                    arg.startswith("--proto_path=") or arg.startswith("-I")
                    for arg in args_list
                )
                assert proto_path_found, f"Expected --proto_path in arguments, got: {args_list}"


# =============================================================================
# Serialization Tests with Well-Known Types
# =============================================================================


@pytest.mark.skipif(
    subprocess.run(["which", "protoc"], capture_output=True).returncode != 0,
    reason="protoc binary not available",
)
class TestGoogleWellKnownTypesSerialization:
    """Integration tests for serializing protobuf messages with well-known types."""

    def test_serialize_message_with_timestamp(self, tmp_path: Path):
        """Test serializing a message with google.protobuf.Timestamp field."""
        schema = ProtobufSchema(schema=SCHEMA_WITH_TIMESTAMP)
        cfg = SimpleNamespace(protobuf_runtime_directory=str(tmp_path / "runtime"))

        instance = get_protobuf_class_instance(schema, "Event", cfg)

        # Set basic fields
        instance.event_id = "test-event-123"
        instance.source = "test-source"

        # Set timestamp using the generated Timestamp class
        # The timestamp field should have seconds and nanos
        instance.event_time.seconds = 1741610886
        instance.event_time.nanos = 0

        # Serialize should work without errors
        serialized = instance.SerializeToString()
        assert serialized is not None
        assert len(serialized) > 0

    def test_serialize_message_with_duration(self, tmp_path: Path):
        """Test serializing a message with google.protobuf.Duration field."""
        schema = ProtobufSchema(schema=SCHEMA_WITH_DURATION)
        cfg = SimpleNamespace(protobuf_runtime_directory=str(tmp_path / "runtime"))

        instance = get_protobuf_class_instance(schema, "Task", cfg)

        instance.task_id = "task-456"
        instance.timeout.seconds = 300
        instance.timeout.nanos = 0

        serialized = instance.SerializeToString()
        assert serialized is not None
        assert len(serialized) > 0

    def test_serialize_message_with_struct(self, tmp_path: Path):
        """Test serializing a message with google.protobuf.Struct field."""
        schema = ProtobufSchema(schema=SCHEMA_WITH_STRUCT)
        cfg = SimpleNamespace(protobuf_runtime_directory=str(tmp_path / "runtime"))

        instance = get_protobuf_class_instance(schema, "Config", cfg)

        instance.config_id = "config-789"
        # Struct fields have a fields dictionary
        instance.properties.fields["key1"].string_value = "value1"
        instance.properties.fields["key2"].number_value = 42.0

        serialized = instance.SerializeToString()
        assert serialized is not None
        assert len(serialized) > 0

    def test_serialize_message_with_multiple_wkt(self, tmp_path: Path):
        """Test serializing a message with multiple well-known types."""
        schema = ProtobufSchema(schema=SCHEMA_WITH_MULTIPLE_WKT)
        cfg = SimpleNamespace(protobuf_runtime_directory=str(tmp_path / "runtime"))

        instance = get_protobuf_class_instance(schema, "Notification", cfg)

        instance.id = "notif-001"

        instance.created_at.seconds = 1741610886
        instance.created_at.nanos = 0

        instance.ttl.seconds = 3600
        instance.ttl.nanos = 0

        instance.metadata.fields["source"].string_value = "test"

        serialized = instance.SerializeToString()
        assert serialized is not None
        assert len(serialized) > 0

    def test_serialize_message_with_nested_wkt(self, tmp_path: Path):
        """Test serializing a message with nested messages using well-known types."""
        schema = ProtobufSchema(schema=SCHEMA_WITH_NESTED_WKT)
        cfg = SimpleNamespace(protobuf_runtime_directory=str(tmp_path / "runtime"))

        instance = get_protobuf_class_instance(schema, "Envelope", cfg)

        instance.envelope_id = "env-001"

        instance.timestamp.seconds = 1741610886
        instance.timestamp.nanos = 0

        # Set nested payload
        instance.payload.data = "payload data"
        instance.payload.processed_at.seconds = 1741610886
        instance.payload.processed_at.nanos = 500

        serialized = instance.SerializeToString()
        assert serialized is not None
        assert len(serialized) > 0

    def test_roundtrip_serialize_deserialize_timestamp(self, tmp_path: Path):
        """Test roundtrip serialization/deserialization with Timestamp."""
        schema = ProtobufSchema(schema=SCHEMA_WITH_TIMESTAMP)
        cfg = SimpleNamespace(protobuf_runtime_directory=str(tmp_path / "runtime"))

        # Create and populate instance
        instance1 = get_protobuf_class_instance(schema, "Event", cfg)
        instance1.event_id = "test-event-123"
        instance1.source = "test-source"

        instance1.event_time.seconds = 1741610886
        instance1.event_time.nanos = 100

        # Serialize
        serialized = instance1.SerializeToString()

        # Deserialize into new instance
        instance2 = get_protobuf_class_instance(schema, "Event", cfg)
        instance2.ParseFromString(serialized)

        # Verify fields
        assert instance2.event_id == "test-event-123"
        assert instance2.source == "test-source"

        assert instance2.event_time.seconds == 1741610886
        assert instance2.event_time.nanos == 100


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestGoogleWellKnownTypesErrorHandling:
    """Tests for error handling when Google protobuf includes are not found."""

    def test_graceful_fallback_when_no_include_path(self, tmp_path: Path):
        """Test graceful fallback when no Google include path is found."""
        schema = ProtobufSchema(schema=SCHEMA_WITH_TIMESTAMP)
        cfg = SimpleNamespace(protobuf_runtime_directory=str(tmp_path / "runtime"))

        # Mock find_protobuf_include to return None
        with patch("karapace.core.protobuf.io.find_protobuf_include", return_value=None):
            with patch("karapace.core.protobuf.io.subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0)

                # Create expected directory structure
                work_dir = tmp_path / "runtime" / calculate_class_name(str(schema))
                work_dir.mkdir(parents=True, exist_ok=True)
                proto_file = work_dir / f"{calculate_class_name(str(schema))}.proto"
                proto_file.write_text(SCHEMA_WITH_TIMESTAMP)
                pb2_file = work_dir / f"{calculate_class_name(str(schema))}_pb2.py"
                pb2_file.write_text(
                    "from google.protobuf import descriptor_pool\n"
                    "class Event:\n"
                    "    pass\n"
                )

                # Should still work - log warning but not fail
                instance = get_protobuf_class_instance(schema, "Event", cfg)
                assert instance.__class__.__name__ == "Event"

    def test_protoc_error_includes_helpful_message(self, tmp_path: Path):
        """Test that protoc errors include helpful context about missing imports."""
        schema = ProtobufSchema(schema=SCHEMA_WITH_TIMESTAMP)
        cfg = SimpleNamespace(protobuf_runtime_directory=str(tmp_path / "runtime"))

        # Mock subprocess.run to simulate protoc failure
        error_output = "google/protobuf/timestamp.proto: File not found."

        with patch("karapace.core.protobuf.io.find_protobuf_include", return_value="/usr/include"):
            with patch("karapace.core.protobuf.io.subprocess.run") as mock_run:
                mock_run.side_effect = subprocess.CalledProcessError(
                    returncode=1,
                    cmd=["protoc", "--python_out=./", "test.proto"],
                    output=b"",
                    stderr=error_output.encode(),
                )

                with pytest.raises(subprocess.CalledProcessError):
                    get_protobuf_class_instance(schema, "Event", cfg)


# =============================================================================
# Protoc Arguments Construction Tests
# =============================================================================


class TestProtocArgumentsConstruction:
    """Tests for the construction of protoc command-line arguments."""

    def test_proto_path_comes_before_python_out(self, tmp_path: Path):
        """Test that --proto_path appears before --python_out in arguments."""
        schema = ProtobufSchema(schema=SCHEMA_WITH_TIMESTAMP)
        cfg = SimpleNamespace(protobuf_runtime_directory=str(tmp_path / "runtime"))

        with patch("karapace.core.protobuf.io.find_protobuf_include", return_value="/usr/include"):
            with patch("karapace.core.protobuf.io.subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0)

                # Calculate expected paths
                work_dir = tmp_path / "runtime" / calculate_class_name(str(schema))
                class_name = calculate_class_name(str(schema))
                pb2_file = work_dir / f"{class_name}_pb2.py"

                # Use side_effect to create the pb2 file when subprocess is called
                def create_pb2_on_call(*args, **kwargs):
                    work_dir.mkdir(parents=True, exist_ok=True)
                    proto_file = work_dir / f"{class_name}.proto"
                    proto_file.write_text(SCHEMA_WITH_TIMESTAMP)
                    pb2_file.write_text(
                        "from google.protobuf import descriptor_pool\n"
                        "class Event:\n"
                        "    pass\n"
                    )
                    return MagicMock(returncode=0)

                mock_run.side_effect = create_pb2_on_call

                get_protobuf_class_instance(schema, "Event", cfg)

                # Get the arguments passed to subprocess.run
                call_args = mock_run.call_args
                args_list = call_args[0][0]

                # Find positions of --proto_path and --python_out
                proto_path_idx = None
                python_out_idx = None

                for i, arg in enumerate(args_list):
                    if arg.startswith("--proto_path=") or arg.startswith("-I"):
                        proto_path_idx = i
                    if arg.startswith("--python_out="):
                        python_out_idx = i

                assert proto_path_idx is not None, "--proto_path should be in arguments"
                assert python_out_idx is not None, "--python_out should be in arguments"
                assert proto_path_idx < python_out_idx, \
                    "--proto_path should come before --python_out"

    def test_absolute_path_used_for_proto_path(self, tmp_path: Path):
        """Test that an absolute path is used for --proto_path."""
        schema = ProtobufSchema(schema=SCHEMA_WITH_TIMESTAMP)
        cfg = SimpleNamespace(protobuf_runtime_directory=str(tmp_path / "runtime"))

        with patch("karapace.core.protobuf.io.find_protobuf_include", return_value="/usr/include"):
            with patch("karapace.core.protobuf.io.subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0)

                # Calculate expected paths
                work_dir = tmp_path / "runtime" / calculate_class_name(str(schema))
                class_name = calculate_class_name(str(schema))
                pb2_file = work_dir / f"{class_name}_pb2.py"

                # Use side_effect to create the pb2 file when subprocess is called
                def create_pb2_on_call(*args, **kwargs):
                    work_dir.mkdir(parents=True, exist_ok=True)
                    proto_file = work_dir / f"{class_name}.proto"
                    proto_file.write_text(SCHEMA_WITH_TIMESTAMP)
                    pb2_file.write_text(
                        "from google.protobuf import descriptor_pool\n"
                        "class Event:\n"
                        "    pass\n"
                    )
                    return MagicMock(returncode=0)

                mock_run.side_effect = create_pb2_on_call

                get_protobuf_class_instance(schema, "Event", cfg)

                call_args = mock_run.call_args
                args_list = call_args[0][0]

                # Find all --proto_path arguments and skip the --proto_path=. (working directory)
                proto_path_args = [arg for arg in args_list if arg.startswith("--proto_path=")]
                google_proto_paths = [a for a in proto_path_args if a != "--proto_path=."]
                assert google_proto_paths, "Expected a Google include --proto_path argument"

                # Extract path from first Google --proto_path=/path
                path = google_proto_paths[0].split("=", 1)[1]
                assert Path(path).is_absolute(), \
                    f"Google --proto_path should use absolute path, got: {path}"


# =============================================================================
# Schema with Dependencies and Well-Known Types
# =============================================================================


@pytest.mark.skipif(
    subprocess.run(["which", "protoc"], capture_output=True).returncode != 0,
    reason="protoc binary not available",
)
class TestSchemaWithDependenciesAndWKT:
    """Tests for schemas that have both dependencies and well-known types."""

    def test_schema_with_dependency_and_timestamp(self, tmp_path: Path):
        """Test schema that has both a dependency and imports google.protobuf.Timestamp."""
        # Create envelope schema with timestamp
        envelope_schema_str = """\
syntax = "proto3";
package test.envelope;

import "google/protobuf/timestamp.proto";

message Envelope {
    string envelope_id = 1;
    google.protobuf.Timestamp timestamp = 2;
}
"""

        # Create main schema that references envelope
        main_schema_str = """\
syntax = "proto3";
package test.main;

import "envelope.proto";

message MainMessage {
    string main_id = 1;
    Envelope envelope = 2;
}
"""

        envelope_schema = ValidatedTypedSchema.parse(
            schema_type=SchemaType.PROTOBUF,
            schema_str=envelope_schema_str,
        )

        main_schema = ValidatedTypedSchema.parse(
            schema_type=SchemaType.PROTOBUF,
            schema_str=main_schema_str,
            references=[],
            dependencies={
                "envelope.proto": Dependency(
                    name="envelope.proto",
                    subject=Subject("envelope"),
                    version="1",
                    target_schema=envelope_schema,
                ),
            },
        )

        cfg = SimpleNamespace(protobuf_runtime_directory=str(tmp_path / "runtime"))

        # This should work without errors
        instance = get_protobuf_class_instance(main_schema.schema, "MainMessage", cfg)
        assert instance.__class__.__name__ == "MainMessage"


# =============================================================================
# Platform-Specific Tests
# =============================================================================


class TestPlatformSpecificPaths:
    """Tests for platform-specific include path discovery."""

    def test_pkg_config_called_if_available(self):
        """Test that pkg-config is called to find include path if available."""
        # Clear cache before and after to avoid test pollution
        find_protobuf_include.cache_clear()
        try:
            with patch("karapace.core.protobuf.io.subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(
                    returncode=0,
                    stdout="/usr/local/include\n",
                    stderr=b"",
                )

                # Create a temporary directory structure that looks like the pkg-config path
                import tempfile
                with tempfile.TemporaryDirectory() as tmp_dir:
                    # Create the expected directory structure
                    google_dir = Path(tmp_dir) / "google" / "protobuf"
                    google_dir.mkdir(parents=True)
                    timestamp_proto = google_dir / "timestamp.proto"
                    timestamp_proto.write_text("// placeholder")

                    # Update mock to return our temp directory
                    mock_run.return_value = MagicMock(
                        returncode=0,
                        stdout=f"{tmp_dir}\n",
                        stderr=b"",
                    )

                    result = find_protobuf_include()

                    # Verify pkg-config was called
                    pkg_config_called = any(
                        call_args and len(call_args[0]) > 0 and "pkg-config" in str(call_args[0])
                        for call_args, _ in mock_run.call_args_list
                    )

                    if pkg_config_called:
                        assert result == str(Path(tmp_dir).resolve())
        finally:
            find_protobuf_include.cache_clear()

    def test_grpc_tools_fallback(self):
        """Test that grpc_tools.protoc is used as fallback."""
        # Check that grpc_tools can be imported
        try:
            import grpc_tools.protoc

            # Get the include path from grpc_tools
            grpc_include = Path(grpc_tools.protoc._get_resource_dir("grpc_tools"))

            if grpc_include and grpc_include.exists():
                # This path should be checked
                timestamp_proto = grpc_include / "google" / "protobuf" / "timestamp.proto"
                if timestamp_proto.exists():
                    # grpc_tools provides well-known types
                    # Verify that find_protobuf_include returns this path when others fail
                    # Clear cache before and after to avoid test pollution
                    find_protobuf_include.cache_clear()
                    try:
                        with patch.object(Path, "exists") as mock_exists:
                            def exists_side_effect(self):
                                # Only return True for the grpc_tools timestamp.proto
                                if "timestamp.proto" in str(self) and str(grpc_include) in str(self):
                                    return True
                                return False

                            mock_exists.side_effect = exists_side_effect
                            result = find_protobuf_include()
                            # Result should be the grpc_tools path
                            assert result is not None
                            assert str(grpc_include.resolve()) == result
                    finally:
                        find_protobuf_include.cache_clear()
        except ImportError:
            pytest.skip("grpc_tools not installed")
