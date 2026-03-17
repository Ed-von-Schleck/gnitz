"""AST -> SMT-LIB2 translator for RPython bitvector functions.

Translates a subset of RPython (straight-line code with r_uint64 arithmetic)
into SMT-LIB2 bitvector formulas suitable for Z3 verification.

Runs under PyPy2.  Uses SSA-style variable numbering: each assignment to a
variable produces a new version wrapped in a (let ...) binding.
"""
import ast
import sys


def _log(msg):
    sys.stderr.write("[smt] %s\n" % msg)


class SmtTranslator(object):
    """Translates a Python2 FunctionDef AST node into an SMT-LIB2 define-fun."""

    def __init__(self, verbose=False):
        self._var_versions = {}   # name -> current SSA version int
        self._lets = []           # [(versioned_name, smt_expr_string), ...]
        self._return_expr = None
        self._param_smts = []
        self._verbose = verbose

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def translate(self, func_node):
        """FunctionDef -> SMT-LIB2 (define-fun ...) string."""
        assert isinstance(func_node, ast.FunctionDef)
        args = func_node.args
        assert len(args.args) >= 1, "expected at least one parameter"

        # Extract parameter names
        param_names = []
        for arg in args.args:
            if isinstance(arg, ast.Name):
                param_names.append(arg.id)
            else:
                param_names.append(arg.arg)  # Python 3 fallback

        param_smts = [name + "_in" for name in param_names]

        if self._verbose:
            _log("translating %s(%s)" % (func_node.name, ", ".join(param_names)))

        # Reset state
        self._var_versions = {}
        self._lets = []
        self._return_expr = None
        self._param_smts = param_smts

        # Seed each parameter as version 0 pointing to its _in symbol
        for i, name in enumerate(param_names):
            self._var_versions[name] = 0
            self._lets.append((self._var_name(name), param_smts[i]))

        for stmt in func_node.body:
            self._translate_stmt(stmt)

        assert self._return_expr is not None, "function has no return statement"
        result = self._build_define_fun(func_node.name, param_smts)
        if self._verbose:
            _log("define-fun output:\n%s" % result)
        return result

    # ------------------------------------------------------------------
    # Statement dispatch
    # ------------------------------------------------------------------

    def _translate_stmt(self, stmt):
        if isinstance(stmt, ast.Return):
            self._return_expr = self._translate_expr(stmt.value)
            if self._verbose:
                _log("  return -> %s" % self._return_expr)
        elif isinstance(stmt, ast.Assign):
            assert len(stmt.targets) == 1
            target = stmt.targets[0]
            assert isinstance(target, ast.Name)
            name = target.id
            smt_val = self._translate_expr(stmt.value)
            new_ver = self._bump_var(name)
            self._lets.append((new_ver, smt_val))
            if self._verbose:
                _log("  assign %s = %s" % (new_ver, smt_val))
        elif isinstance(stmt, ast.AugAssign):
            target = stmt.target
            assert isinstance(target, ast.Name)
            name = target.id
            cur = self._var_name(name)
            rhs = self._translate_expr(stmt.value)
            smt_val = self._binop_smt(stmt.op, cur, rhs)
            new_ver = self._bump_var(name)
            self._lets.append((new_ver, smt_val))
            if self._verbose:
                _log("  augassign %s = %s" % (new_ver, smt_val))
        elif isinstance(stmt, ast.Expr):
            pass  # standalone expressions (docstrings, etc.) — skip
        else:
            raise ValueError("unsupported statement: %s" % type(stmt).__name__)

    # ------------------------------------------------------------------
    # Expression translation
    # ------------------------------------------------------------------

    def _translate_expr(self, node):
        if isinstance(node, ast.Name):
            return self._var_name(node.id)
        elif isinstance(node, ast.Num):
            return self._format_num(node.n)
        elif isinstance(node, ast.BinOp):
            left = self._translate_expr(node.left)
            right = self._translate_expr(node.right)
            return self._binop_smt(node.op, left, right)
        elif isinstance(node, ast.Call):
            # r_uint64(x) and intmask(x) are identity — BV64 ops already wrap
            func = node.func
            if isinstance(func, ast.Name) and func.id in ("r_uint64", "intmask"):
                assert len(node.args) == 1
                return self._translate_expr(node.args[0])
            raise ValueError("unsupported call: %s" % ast.dump(func))
        else:
            raise ValueError("unsupported expr: %s" % type(node).__name__)

    def _binop_smt(self, op, left, right):
        if isinstance(op, ast.BitXor):
            return "(bvxor %s %s)" % (left, right)
        elif isinstance(op, ast.RShift):
            return "(bvlshr %s %s)" % (left, right)
        elif isinstance(op, ast.Mult):
            return "(bvmul %s %s)" % (left, right)
        elif isinstance(op, ast.LShift):
            return "(bvshl %s %s)" % (left, right)
        elif isinstance(op, ast.BitAnd):
            return "(bvand %s %s)" % (left, right)
        elif isinstance(op, ast.BitOr):
            return "(bvor %s %s)" % (left, right)
        elif isinstance(op, ast.Add):
            return "(bvadd %s %s)" % (left, right)
        elif isinstance(op, ast.Sub):
            return "(bvsub %s %s)" % (left, right)
        else:
            raise ValueError("unsupported binop: %s" % type(op).__name__)

    # ------------------------------------------------------------------
    # SSA variable management
    # ------------------------------------------------------------------

    def _var_name(self, name):
        ver = self._var_versions.get(name)
        if ver is None:
            raise ValueError("undefined variable: %s" % name)
        return "%s_%d" % (name, ver)

    def _bump_var(self, name):
        ver = self._var_versions.get(name, -1) + 1
        self._var_versions[name] = ver
        return "%s_%d" % (name, ver)

    # ------------------------------------------------------------------
    # Number formatting
    # ------------------------------------------------------------------

    @staticmethod
    def _format_num(n):
        """Int or long -> SMT-LIB2 64-bit bitvector literal."""
        # Mask to 64 bits (handles Python longs and negative values)
        n = n & 0xFFFFFFFFFFFFFFFF
        if n <= 0xFFFF:
            return "(_ bv%d 64)" % n
        return "#x%016x" % n

    # ------------------------------------------------------------------
    # Output assembly
    # ------------------------------------------------------------------

    def get_param_names(self):
        """Return the list of SMT parameter names (e.g., ['n_in', 'c_in'])."""
        return list(self._param_smts)

    def _build_define_fun(self, func_name, param_smts):
        if isinstance(param_smts, str):
            param_smts = [param_smts]  # backward compat
        param_decls = " ".join("(%s (_ BitVec 64))" % p for p in param_smts)
        lines = []
        lines.append("(define-fun %s (%s) (_ BitVec 64)" % (func_name, param_decls))
        indent = "  "
        for var_name, smt_expr in self._lets:
            lines.append("%s(let ((%s %s))" % (indent, var_name, smt_expr))
            indent += ""
        lines.append("%s  %s%s)" % (indent, self._return_expr, ")" * len(self._lets)))
        return "\n".join(lines)

    def get_let_body(self):
        """Return the nested let-body (without define-fun wrapper).

        Useful for inlining with a concrete input value for cross-checking.
        """
        parts = []
        for var_name, smt_expr in self._lets:
            parts.append("(let ((%s %s))" % (var_name, smt_expr))
        parts.append("  %s%s" % (self._return_expr, ")" * len(self._lets)))
        return "\n".join(parts)

    def get_constants(self):
        """Extract hex constants from the let bindings.

        Returns a list of hex strings (e.g., ['#xff51afd7ed558ccd', ...])
        found in bvmul operations.
        """
        constants = []
        for _, smt_expr in self._lets:
            if "bvmul" in smt_expr:
                # Extract #x... constant from the expression
                idx = smt_expr.find("#x")
                if idx >= 0:
                    hex_str = smt_expr[idx:idx + 18]  # #x + 16 hex digits
                    constants.append(hex_str)
        return constants
