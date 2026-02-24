# tests/catalog/test_catalog_identifiers.py

import unittest
from gnitz.catalog import identifiers
from gnitz.core.errors import LayoutError


class TestCatalogIdentifiers(unittest.TestCase):
    def test_validate_user_identifier_success(self):
        """Verifies that valid alphanumeric identifiers are accepted."""
        valid_names = [
            "orders",
            "Orders123",
            "my_table",
            "a",
            "A1_b2",
            "1a",  # Digits at start are allowed by the spec, only '_' is reserved
            "99_problems",
        ]
        for name in valid_names:
            # Should not raise
            identifiers.validate_user_identifier(name)

    def test_validate_user_identifier_reserved_prefix(self):
        """Verifies that identifiers starting with '_' are rejected."""
        reserved_names = ["_private", "_", "_system", "__init__"]
        for name in reserved_names:
            with self.assertRaises(LayoutError) as cm:
                identifiers.validate_user_identifier(name)
            self.assertIn("reserved for system prefix", str(cm.exception))

    def test_validate_user_identifier_empty(self):
        """Verifies that empty identifiers are rejected."""
        with self.assertRaises(LayoutError) as cm:
            identifiers.validate_user_identifier("")
        self.assertIn("cannot be empty", str(cm.exception))

    def test_validate_user_identifier_invalid_chars(self):
        """Verifies that non-alphanumeric characters are rejected."""
        invalid_names = [
            "has space",
            "has-dash",
            "has.dot",
            "has@",
            "naïve",  # Non-ASCII
            "table$",
        ]
        for name in invalid_names:
            with self.assertRaises(LayoutError) as cm:
                identifiers.validate_user_identifier(name)
            self.assertIn("invalid characters", str(cm.exception))

    def test_parse_qualified_name(self):
        """Verifies splitting of schema.entity names."""
        # Simple name uses default
        self.assertEqual(
            identifiers.parse_qualified_name("orders", "public"), ("public", "orders")
        )

        # Explicit schema
        self.assertEqual(
            identifiers.parse_qualified_name("sales.orders", "public"),
            ("sales", "orders"),
        )

        # System qualified names (parsing should work regardless of validation rules)
        self.assertEqual(
            identifiers.parse_qualified_name("_system._tables", "public"),
            ("_system", "_tables"),
        )

        # Multiple dots: split on the first dot only
        self.assertEqual(
            identifiers.parse_qualified_name("a.b.c", "public"), ("a", "b.c")
        )

        # Boundary: empty entity (validator catches this later, but parser should split)
        self.assertEqual(
            identifiers.parse_qualified_name("myschema.", "public"), ("myschema", "")
        )
