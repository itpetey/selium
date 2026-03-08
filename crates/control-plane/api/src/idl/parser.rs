use crate::ApiError;

use super::ast::{
    AstEventDef, AstFieldDef, AstPackageDecl, AstProperty, AstSchemaDef, AstServiceDef,
    AstStreamDef, IdlDocument, Span, Symbol, Token, TokenKind, ValueToken, describe_symbol,
    describe_token, parse_error_at,
};

pub(super) struct Parser {
    tokens: Vec<Token>,
    index: usize,
}

impl Parser {
    pub(super) fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, index: 0 }
    }

    pub(super) fn parse_document(&mut self) -> std::result::Result<IdlDocument, ApiError> {
        let mut document = IdlDocument {
            package: None,
            schemas: Vec::new(),
            events: Vec::new(),
            services: Vec::new(),
            streams: Vec::new(),
        };

        self.skip_newlines();
        while !self.at_eof() {
            let keyword = self.peek_ident().ok_or_else(|| {
                parse_error_at(
                    self.current().span,
                    format!(
                        "unexpected top-level token {}",
                        describe_token(&self.current().kind)
                    ),
                )
            })?;

            match keyword {
                "package" => {
                    let package = self.parse_package_decl()?;
                    if document.package.replace(package).is_some() {
                        return Err(parse_error_at(
                            self.previous_span(),
                            "duplicate package declaration".to_string(),
                        ));
                    }
                }
                "schema" => document.schemas.push(self.parse_schema_def()?),
                "event" => document.events.push(self.parse_event_def()?),
                "service" => document.services.push(self.parse_service_def()?),
                "stream" => document.streams.push(self.parse_stream_def()?),
                _ => {
                    return Err(parse_error_at(
                        self.current().span,
                        format!("unexpected top-level statement `{keyword}`"),
                    ));
                }
            }

            self.skip_newlines();
        }

        Ok(document)
    }

    fn parse_package_decl(&mut self) -> std::result::Result<AstPackageDecl, ApiError> {
        let span = self.expect_keyword("package")?;
        let (name, _) = self.expect_ident("package name")?;
        self.finish_item("package declaration")?;
        Ok(AstPackageDecl { name, span })
    }

    fn parse_schema_def(&mut self) -> std::result::Result<AstSchemaDef, ApiError> {
        let span = self.expect_keyword("schema")?;
        let (name, _) = self.expect_ident("schema name")?;
        self.expect_symbol(Symbol::LBrace, "schema block")?;
        let fields = self.parse_block(|parser| parser.parse_field_def(), "schema field")?;
        self.finish_item("schema block")?;
        Ok(AstSchemaDef { name, span, fields })
    }

    fn parse_field_def(&mut self) -> std::result::Result<AstFieldDef, ApiError> {
        let (name, span) = self.expect_ident("field name")?;
        self.expect_symbol(Symbol::Colon, "field type separator")?;
        let (ty, _) = self.expect_ident("field type")?;
        Ok(AstFieldDef { name, ty, span })
    }

    fn parse_event_def(&mut self) -> std::result::Result<AstEventDef, ApiError> {
        let span = self.expect_keyword("event")?;
        let (name, _) = self.expect_ident("event name")?;
        self.expect_symbol(Symbol::LParen, "event payload")?;
        let (payload_schema, _) = self.expect_ident("event payload schema")?;
        self.expect_symbol(Symbol::RParen, "event payload")?;
        self.expect_symbol(Symbol::LBrace, "event block")?;
        let properties = self.parse_block(|parser| parser.parse_property(), "event property")?;
        self.finish_item("event block")?;
        Ok(AstEventDef {
            name,
            payload_schema,
            span,
            properties,
        })
    }

    fn parse_service_def(&mut self) -> std::result::Result<AstServiceDef, ApiError> {
        let span = self.expect_keyword("service")?;
        let (name, _) = self.expect_ident("service name")?;
        self.expect_symbol(Symbol::LParen, "service request schema")?;
        let (request_schema, _) = self.expect_ident("service request schema")?;
        self.expect_symbol(Symbol::RParen, "service request schema")?;
        self.expect_symbol(Symbol::Arrow, "service response schema")?;
        let (response_schema, _) = self.expect_ident("service response schema")?;

        let properties = if self.consume_symbol(Symbol::LBrace) {
            let properties =
                self.parse_block(|parser| parser.parse_property(), "service property")?;
            self.finish_item("service block")?;
            properties
        } else {
            self.finish_item("service declaration")?;
            Vec::new()
        };

        Ok(AstServiceDef {
            name,
            request_schema,
            response_schema,
            span,
            properties,
        })
    }

    fn parse_stream_def(&mut self) -> std::result::Result<AstStreamDef, ApiError> {
        let span = self.expect_keyword("stream")?;
        let (name, _) = self.expect_ident("stream name")?;
        self.expect_symbol(Symbol::LParen, "stream payload schema")?;
        let (payload_schema, _) = self.expect_ident("stream payload schema")?;
        self.expect_symbol(Symbol::RParen, "stream payload schema")?;
        self.finish_item("stream declaration")?;
        Ok(AstStreamDef {
            name,
            payload_schema,
            span,
        })
    }

    fn parse_property(&mut self) -> std::result::Result<AstProperty, ApiError> {
        let (key, span) = self.expect_ident("property name")?;
        self.expect_symbol(Symbol::Colon, "property value separator")?;

        let mut value = Vec::new();
        while !self.at_eof() && !self.check_symbol(Symbol::RBrace) && !self.check_terminator() {
            value.push(self.parse_value_token()?);
        }

        Ok(AstProperty { key, span, value })
    }

    fn parse_value_token(&mut self) -> std::result::Result<ValueToken, ApiError> {
        let token = self.advance();
        match token.kind {
            TokenKind::Ident(value) => Ok(ValueToken::Ident(value)),
            TokenKind::String(value) => Ok(ValueToken::String(value)),
            TokenKind::Symbol(Symbol::LAngle) => Ok(ValueToken::LAngle),
            TokenKind::Symbol(Symbol::RAngle) => Ok(ValueToken::RAngle),
            TokenKind::Symbol(Symbol::Eq) => Ok(ValueToken::Eq),
            other => Err(parse_error_at(
                token.span,
                format!(
                    "unexpected token {} in property value",
                    describe_token(&other)
                ),
            )),
        }
    }

    fn parse_block<T>(
        &mut self,
        mut parse_item: impl FnMut(&mut Self) -> std::result::Result<T, ApiError>,
        context: &str,
    ) -> std::result::Result<Vec<T>, ApiError> {
        let mut items = Vec::new();
        self.skip_newlines();

        while !self.consume_symbol(Symbol::RBrace) {
            if self.at_eof() {
                return Err(parse_error_at(
                    self.current().span,
                    format!("unexpected end of input while parsing {context}"),
                ));
            }

            items.push(parse_item(self)?);

            if self.check_symbol(Symbol::RBrace) {
                continue;
            }

            let consumed = self.consume_terminators();
            if consumed == 0 && !self.check_symbol(Symbol::RBrace) {
                return Err(parse_error_at(
                    self.current().span,
                    format!("expected `;`, newline, or `}}` after {context}"),
                ));
            }
            self.skip_newlines();
        }

        Ok(items)
    }

    fn finish_item(&mut self, context: &str) -> std::result::Result<(), ApiError> {
        if self.at_eof() {
            return Ok(());
        }

        let consumed = self.consume_terminators();
        if consumed > 0 || self.at_eof() {
            return Ok(());
        }

        Err(parse_error_at(
            self.current().span,
            format!("expected `;` or newline after {context}"),
        ))
    }

    fn consume_terminators(&mut self) -> usize {
        let mut consumed = 0;
        while self.consume_symbol(Symbol::Semi) || self.consume_newline() {
            consumed += 1;
        }
        consumed
    }

    fn skip_newlines(&mut self) {
        while self.consume_newline() {}
    }

    fn expect_keyword(&mut self, keyword: &str) -> std::result::Result<Span, ApiError> {
        let token = self.advance();
        match token.kind {
            TokenKind::Ident(value) if value == keyword => Ok(token.span),
            other => Err(parse_error_at(
                token.span,
                format!("expected `{keyword}`, found {}", describe_token(&other)),
            )),
        }
    }

    fn expect_ident(&mut self, context: &str) -> std::result::Result<(String, Span), ApiError> {
        let token = self.advance();
        match token.kind {
            TokenKind::Ident(value) => Ok((value, token.span)),
            other => Err(parse_error_at(
                token.span,
                format!("expected {context}, found {}", describe_token(&other)),
            )),
        }
    }

    fn expect_symbol(
        &mut self,
        symbol: Symbol,
        context: &str,
    ) -> std::result::Result<Span, ApiError> {
        let token = self.advance();
        match token.kind {
            TokenKind::Symbol(found) if found == symbol => Ok(token.span),
            other => Err(parse_error_at(
                token.span,
                format!(
                    "expected {} for {context}, found {}",
                    describe_symbol(symbol),
                    describe_token(&other)
                ),
            )),
        }
    }

    fn peek_ident(&self) -> Option<&str> {
        match &self.current().kind {
            TokenKind::Ident(value) => Some(value.as_str()),
            _ => None,
        }
    }

    fn check_symbol(&self, symbol: Symbol) -> bool {
        matches!(&self.current().kind, TokenKind::Symbol(found) if *found == symbol)
    }

    fn check_terminator(&self) -> bool {
        matches!(
            self.current().kind,
            TokenKind::Newline | TokenKind::Symbol(Symbol::Semi)
        )
    }

    fn consume_symbol(&mut self, symbol: Symbol) -> bool {
        if self.check_symbol(symbol) {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn consume_newline(&mut self) -> bool {
        if matches!(self.current().kind, TokenKind::Newline) {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn current(&self) -> &Token {
        &self.tokens[self.index]
    }

    fn previous_span(&self) -> Span {
        self.tokens[self.index.saturating_sub(1)].span
    }

    fn advance(&mut self) -> Token {
        let token = self.current().clone();
        if !matches!(token.kind, TokenKind::Eof) {
            self.index += 1;
        }
        token
    }

    fn at_eof(&self) -> bool {
        matches!(self.current().kind, TokenKind::Eof)
    }
}

#[cfg(test)]
mod tests {
    use super::Parser;
    use crate::idl::ast::ValueToken;
    use crate::idl::lexer::Lexer;

    #[test]
    fn parses_service_properties_into_value_tokens() {
        let tokens = Lexer::new(
            r#"
package media.pipeline.v1
service upload(Req) -> Res {
  request-body: stream<bytes>
}
"#,
        )
        .tokenize()
        .expect("tokenize");

        let document = Parser::new(tokens).parse_document().expect("parse");
        let service = &document.services[0];

        assert_eq!(service.name, "upload");
        assert_eq!(
            service.properties[0].value,
            vec![
                ValueToken::Ident("stream".to_string()),
                ValueToken::LAngle,
                ValueToken::Ident("bytes".to_string()),
                ValueToken::RAngle,
            ]
        );
    }

    #[test]
    fn rejects_missing_field_terminators_inside_blocks() {
        let tokens = Lexer::new(
            r#"
package media.pipeline.v1
schema Frame {
  camera_id: string ts_ms: u64
}
"#,
        )
        .tokenize()
        .expect("tokenize");

        let err = Parser::new(tokens).parse_document().expect_err("must fail");
        assert!(
            err.to_string()
                .contains("expected `;`, newline, or `}` after schema field")
        );
    }
}
