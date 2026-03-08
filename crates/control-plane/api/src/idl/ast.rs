use crate::ApiError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Span {
    pub(super) line: usize,
    pub(super) column: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Symbol {
    LBrace,
    RBrace,
    LParen,
    RParen,
    LAngle,
    RAngle,
    Colon,
    Semi,
    Arrow,
    Eq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum TokenKind {
    Ident(String),
    String(String),
    Symbol(Symbol),
    Newline,
    Eof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Token {
    pub(super) kind: TokenKind,
    pub(super) span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ValueToken {
    Ident(String),
    String(String),
    LAngle,
    RAngle,
    Eq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct IdlDocument {
    pub(super) package: Option<AstPackageDecl>,
    pub(super) schemas: Vec<AstSchemaDef>,
    pub(super) events: Vec<AstEventDef>,
    pub(super) services: Vec<AstServiceDef>,
    pub(super) streams: Vec<AstStreamDef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AstPackageDecl {
    pub(super) name: String,
    pub(super) span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AstSchemaDef {
    pub(super) name: String,
    pub(super) span: Span,
    pub(super) fields: Vec<AstFieldDef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AstFieldDef {
    pub(super) name: String,
    pub(super) ty: String,
    pub(super) span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AstEventDef {
    pub(super) name: String,
    pub(super) payload_schema: String,
    pub(super) span: Span,
    pub(super) properties: Vec<AstProperty>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AstServiceDef {
    pub(super) name: String,
    pub(super) request_schema: String,
    pub(super) response_schema: String,
    pub(super) span: Span,
    pub(super) properties: Vec<AstProperty>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AstStreamDef {
    pub(super) name: String,
    pub(super) payload_schema: String,
    pub(super) span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AstProperty {
    pub(super) key: String,
    pub(super) span: Span,
    pub(super) value: Vec<ValueToken>,
}

pub(super) fn is_ident_start(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

pub(super) fn is_ident_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '-' | '/')
}

pub(super) fn parse_error_at(span: Span, message: String) -> ApiError {
    ApiError::Parse(format!("{message} at {}:{}", span.line, span.column))
}

pub(super) fn describe_token(token: &TokenKind) -> String {
    match token {
        TokenKind::Ident(value) => format!("identifier `{value}`"),
        TokenKind::String(value) => format!("string literal {value:?}"),
        TokenKind::Symbol(symbol) => format!("symbol `{}`", describe_symbol(*symbol)),
        TokenKind::Newline => "newline".to_string(),
        TokenKind::Eof => "end of input".to_string(),
    }
}

pub(super) fn describe_symbol(symbol: Symbol) -> &'static str {
    match symbol {
        Symbol::LBrace => "{",
        Symbol::RBrace => "}",
        Symbol::LParen => "(",
        Symbol::RParen => ")",
        Symbol::LAngle => "<",
        Symbol::RAngle => ">",
        Symbol::Colon => ":",
        Symbol::Semi => ";",
        Symbol::Arrow => "->",
        Symbol::Eq => "=",
    }
}

pub(super) fn render_value_tokens(tokens: &[ValueToken]) -> String {
    let mut rendered = String::new();
    for token in tokens {
        match token {
            ValueToken::Ident(value) => rendered.push_str(value),
            ValueToken::String(value) => rendered.push_str(&format!("{value:?}")),
            ValueToken::LAngle => rendered.push('<'),
            ValueToken::RAngle => rendered.push('>'),
            ValueToken::Eq => rendered.push('='),
        }
        rendered.push(' ');
    }
    rendered.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::{ValueToken, is_ident_char, is_ident_start, render_value_tokens};

    #[test]
    fn identifier_character_rules_match_idl_expectations() {
        assert!(is_ident_start('a'));
        assert!(is_ident_start('1'));
        assert!(is_ident_start('_'));
        assert!(!is_ident_start('-'));

        assert!(is_ident_char('.'));
        assert!(is_ident_char('-'));
        assert!(is_ident_char('/'));
        assert!(!is_ident_char(':'));
    }

    #[test]
    fn render_value_tokens_preserves_nested_type_shape() {
        let rendered = render_value_tokens(&[
            ValueToken::Ident("stream".to_string()),
            ValueToken::LAngle,
            ValueToken::String("bytes".to_string()),
            ValueToken::RAngle,
        ]);

        assert_eq!(rendered, "stream < \"bytes\" >");
    }
}
