//! The dialect statements are parsed and tokenized under.

use sqlparser::ast::Expr;
use sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;
use std::any::TypeId;

/// The dialect every statement is parsed under: `GenericDialect`, plus a
/// prefix hook for literal tokens. A client that tokenizes text it will submit
/// uses this one, so it splits statements where the parser does.
///
/// Before every expression prefix sqlparser first tries to parse a data type
/// (for `DATE '…'`-style typed strings), and that attempt builds and drops a
/// formatted error whenever it fails. It cannot succeed on a number or a
/// single-quoted string token, whose default arm is a plain `parse_value`, so
/// `parse_prefix` parses those directly.
///
/// `dialect()` reports `GenericDialect`'s type id, so sqlparser's
/// `dialect_of!` checks branch as for `GenericDialect`. Every other method
/// that `GenericDialect` overrides forwards to it; a test checks that list
/// against the source of the sqlparser this crate resolves.
#[derive(Debug, Default, Clone, Copy)]
pub struct GnitzDialect;

macro_rules! forward {
    ($($m:ident),* $(,)?) => {
        $(fn $m(&self) -> bool { GenericDialect {}.$m() })*
    };
}

impl Dialect for GnitzDialect {
    fn dialect(&self) -> TypeId {
        TypeId::of::<GenericDialect>()
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        GenericDialect {}.is_delimited_identifier_start(ch)
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        GenericDialect {}.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        GenericDialect {}.is_identifier_part(ch)
    }

    fn parse_prefix(&self, parser: &mut Parser) -> Option<Result<Expr, ParserError>> {
        match parser.peek_token_ref().token {
            Token::Number(..) | Token::SingleQuotedString(_) => Some(parser.parse_value().map(Expr::Value)),
            _ => None,
        }
    }

    forward!(
        supports_unicode_string_literal,
        supports_partition_by_after_order_by,
        supports_array_join_syntax,
        supports_group_by_expr,
        supports_group_by_with_modifier,
        supports_left_associative_joins_without_parens,
        supports_connect_by,
        supports_match_recognize,
        supports_pipe_operator,
        supports_start_transaction_modifier,
        supports_window_function_null_treatment_arg,
        supports_dictionary_syntax,
        supports_window_clause_named_window_reference,
        supports_parenthesized_set_variables,
        supports_select_wildcard_except,
        support_map_literal_syntax,
        allow_extract_custom,
        allow_extract_single_quotes,
        supports_extract_comma_syntax,
        supports_create_view_comment_syntax,
        supports_parens_around_table_factor,
        supports_values_as_table_factor,
        supports_create_index_with_clause,
        supports_explain_with_utility_options,
        supports_limit_comma,
        supports_update_order_by,
        supports_from_first_select,
        supports_projection_trailing_commas,
        supports_asc_desc_in_column_definition,
        supports_try_convert,
        supports_bitwise_shift_operators,
        supports_comment_on,
        supports_load_extension,
        supports_named_fn_args_with_assignment_operator,
        supports_struct_literal,
        supports_empty_projections,
        supports_nested_comments,
        supports_multiline_comment_hints,
        supports_user_host_grantee,
        supports_string_escape_constant,
        supports_array_typedef_with_brackets,
        supports_match_against,
        supports_set_names,
        supports_comma_separated_set_assignments,
        supports_filter_during_aggregation,
        supports_select_wildcard_exclude,
        supports_data_type_signed_suffix,
        supports_interval_options,
        supports_quote_delimited_string,
        supports_select_wildcard_replace,
        supports_select_wildcard_ilike,
        supports_select_wildcard_rename,
        supports_optimize_table,
        supports_install,
        supports_detach,
        supports_prewhere,
        supports_with_fill,
        supports_limit_by,
        supports_interpolate,
        supports_settings,
        supports_select_format,
        supports_comment_optimizer_hint,
        supports_constraint_keyword_without_name,
        supports_key_column_option,
        supports_comma_separated_trim,
        supports_cte_without_as,
        supports_select_item_multi_column_alias,
        supports_xml_expressions,
    );
}

#[cfg(test)]
#[path = "tests/dialect.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/dialect.rs"]
mod bench;
