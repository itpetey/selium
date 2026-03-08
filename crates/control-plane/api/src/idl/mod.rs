mod ast;
mod lexer;
mod lower;
mod parser;
mod validate;

use crate::{ApiError, ContractPackage};

use self::lexer::Lexer;
use self::lower::lower_document;
use self::parser::Parser;

pub fn parse_idl(input: &str) -> std::result::Result<ContractPackage, ApiError> {
    let tokens = Lexer::new(input).tokenize()?;
    let document = Parser::new(tokens).parse_document()?;
    lower_document(document)
}

#[cfg(test)]
mod tests {
    use super::parse_idl;

    #[test]
    fn parse_idl_smoke_test_exercises_pipeline() {
        let package = parse_idl(
            r#"
package media.pipeline.v1

schema Frame {
  camera_id: string
}

service detect(Frame) -> Frame
"#,
        )
        .expect("parse");

        assert_eq!(package.namespace, "media.pipeline");
        assert_eq!(package.services[0].name, "detect");
    }
}
