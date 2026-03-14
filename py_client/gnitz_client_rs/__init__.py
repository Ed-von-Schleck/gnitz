"""
Thin compatibility shim: exposes the gnitz ergonomics layer under
gnitz_client-compatible names.
"""
import gnitz

GnitzClient = gnitz.Connection
GnitzError  = gnitz.GnitzError
TypeCode    = gnitz.TypeCode
ColumnDef   = gnitz.ColumnDef
Schema      = gnitz.Schema

__all__ = ["GnitzClient", "GnitzError", "TypeCode", "ColumnDef", "Schema"]
