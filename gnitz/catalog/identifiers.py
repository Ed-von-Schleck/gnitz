# gnitz/catalog/identifiers.py

from gnitz.core.errors import LayoutError


def _is_valid_ident_char(ch):
    """
    Returns True iff ch is in [a-zA-Z0-9_].
    Implemented using range checks for RPython safety.
    """
    if "a" <= ch <= "z":
        return True
    if "A" <= ch <= "Z":
        return True
    if "0" <= ch <= "9":
        return True
    if ch == "_":
        return True
    return False


def validate_user_identifier(name):
    """
    Validates that a string is a valid user-defined identifier.
    Raises LayoutError if:
      - name is empty
      - name starts with '_' (reserved for system)
      - any character is not alphanumeric or underscore
    """
    length = len(name)
    if length == 0:
        raise LayoutError("Identifier cannot be empty")

    if name[0] == "_":
        raise LayoutError(
            "User identifiers cannot start with '_' (reserved for system prefix): %s"
            % name
        )

    for i in range(length):
        if not _is_valid_ident_char(name[i]):
            raise LayoutError("Identifier contains invalid characters: %s" % name)


def _find_dot(s):
    """
    Returns the index of the first '.' in the string, or -1.
    Manual loop to ensure the RPython annotator can prove bounds.
    """
    for i in range(len(s)):
        if s[i] == ".":
            return i
    return -1


def parse_qualified_name(name, default_schema):
    """
    Parses a potentially qualified name into a (schema, entity) tuple.
    Example:
      "orders"            -> ("public", "orders")
      "sales.orders"      -> ("sales", "orders")
      "_system._tables"   -> ("_system", "_tables")
    """
    dot_pos = _find_dot(name)
    if dot_pos >= 0:
        # Explicit assertion for the RPython annotator to prove non-negative
        # indices before slicing.
        assert dot_pos >= 0
        schema_part = name[:dot_pos]
        entity_part = name[dot_pos + 1 :]
        return schema_part, entity_part
    else:
        return default_schema, name
