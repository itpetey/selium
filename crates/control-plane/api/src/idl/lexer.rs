use crate::ApiError;

use super::ast::{Span, Symbol, Token, TokenKind, is_ident_char, is_ident_start, parse_error_at};

pub(super) struct Lexer<'a> {
    input: &'a str,
    offset: usize,
    line: usize,
    column: usize,
}

impl<'a> Lexer<'a> {
    pub(super) fn new(input: &'a str) -> Self {
        Self {
            input,
            offset: 0,
            line: 1,
            column: 1,
        }
    }

    pub(super) fn tokenize(mut self) -> std::result::Result<Vec<Token>, ApiError> {
        let mut tokens = Vec::new();

        while let Some(ch) = self.peek_char() {
            let span = self.span();
            match ch {
                ' ' | '\t' | '\r' => {
                    self.bump_char();
                }
                '\n' => {
                    self.bump_char();
                    tokens.push(Token {
                        kind: TokenKind::Newline,
                        span,
                    });
                }
                '/' if self.peek_next_char() == Some('/') => {
                    self.skip_comment();
                }
                '{' => {
                    self.bump_char();
                    tokens.push(self.symbol_token(Symbol::LBrace, span));
                }
                '}' => {
                    self.bump_char();
                    tokens.push(self.symbol_token(Symbol::RBrace, span));
                }
                '(' => {
                    self.bump_char();
                    tokens.push(self.symbol_token(Symbol::LParen, span));
                }
                ')' => {
                    self.bump_char();
                    tokens.push(self.symbol_token(Symbol::RParen, span));
                }
                '<' => {
                    self.bump_char();
                    tokens.push(self.symbol_token(Symbol::LAngle, span));
                }
                '>' => {
                    self.bump_char();
                    tokens.push(self.symbol_token(Symbol::RAngle, span));
                }
                ':' => {
                    self.bump_char();
                    tokens.push(self.symbol_token(Symbol::Colon, span));
                }
                ';' => {
                    self.bump_char();
                    tokens.push(self.symbol_token(Symbol::Semi, span));
                }
                '=' => {
                    self.bump_char();
                    tokens.push(self.symbol_token(Symbol::Eq, span));
                }
                '-' if self.peek_next_char() == Some('>') => {
                    self.bump_char();
                    self.bump_char();
                    tokens.push(self.symbol_token(Symbol::Arrow, span));
                }
                '"' => tokens.push(self.lex_string()?),
                _ if is_ident_start(ch) => tokens.push(self.lex_ident()),
                _ => {
                    return Err(parse_error_at(span, format!("unexpected character `{ch}`")));
                }
            }
        }

        tokens.push(Token {
            kind: TokenKind::Eof,
            span: self.span(),
        });
        Ok(tokens)
    }

    fn lex_string(&mut self) -> std::result::Result<Token, ApiError> {
        let span = self.span();
        self.bump_char();
        let mut value = String::new();

        while let Some(ch) = self.peek_char() {
            match ch {
                '"' => {
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::String(value),
                        span,
                    });
                }
                '\\' => {
                    self.bump_char();
                    let escaped = self.peek_char().ok_or_else(|| {
                        parse_error_at(span, "unterminated string literal".to_string())
                    })?;
                    let value_ch = match escaped {
                        '"' => '"',
                        '\\' => '\\',
                        'n' => '\n',
                        'r' => '\r',
                        't' => '\t',
                        other => other,
                    };
                    self.bump_char();
                    value.push(value_ch);
                }
                _ => {
                    self.bump_char();
                    value.push(ch);
                }
            }
        }

        Err(parse_error_at(
            span,
            "unterminated string literal".to_string(),
        ))
    }

    fn lex_ident(&mut self) -> Token {
        let span = self.span();
        let mut value = String::new();
        while let Some(ch) = self.peek_char() {
            if !is_ident_char(ch) {
                break;
            }
            self.bump_char();
            value.push(ch);
        }

        Token {
            kind: TokenKind::Ident(value),
            span,
        }
    }

    fn symbol_token(&self, symbol: Symbol, span: Span) -> Token {
        Token {
            kind: TokenKind::Symbol(symbol),
            span,
        }
    }

    fn skip_comment(&mut self) {
        while let Some(ch) = self.peek_char() {
            if ch == '\n' {
                break;
            }
            self.bump_char();
        }
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.offset..].chars().next()
    }

    fn peek_next_char(&self) -> Option<char> {
        let mut chars = self.input[self.offset..].chars();
        chars.next()?;
        chars.next()
    }

    fn bump_char(&mut self) -> Option<char> {
        let ch = self.peek_char()?;
        self.offset += ch.len_utf8();
        if ch == '\n' {
            self.line += 1;
            self.column = 1;
        } else {
            self.column += 1;
        }
        Some(ch)
    }

    fn span(&self) -> Span {
        Span {
            line: self.line,
            column: self.column,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Lexer;
    use crate::idl::ast::{Symbol, TokenKind};

    #[test]
    fn tokenizes_comments_without_touching_string_literals() {
        let tokens = Lexer::new(
            r#"
service upload(Req) -> Res {
  path: "/upload//raw"
  // trailing comment
}
"#,
        )
        .tokenize()
        .expect("tokenize");

        assert!(
            tokens
                .iter()
                .any(|token| token.kind == TokenKind::String("/upload//raw".to_string()))
        );
        assert!(
            tokens
                .iter()
                .any(|token| token.kind == TokenKind::Symbol(Symbol::LBrace))
        );
    }

    #[test]
    fn reports_unterminated_string_literals() {
        let err = Lexer::new(r#"service upload(Req) -> Res { path: "/broken }"#)
            .tokenize()
            .expect_err("must fail");

        assert!(err.to_string().contains("unterminated string literal"));
    }
}
