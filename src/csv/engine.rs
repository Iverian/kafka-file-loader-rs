use std::sync::Arc;

use rhai::packages::Package;
use rhai::packages::StandardPackage;
use rhai::{Engine, OptimizationLevel, Scope, AST};
use stable_eyre::eyre::{Context, Result};

#[derive(Clone, Debug)]
pub struct ExprEngine {
    inner: Arc<Engine>,
}

#[derive(Clone, Debug)]
pub struct Expression {
    parent: Arc<Engine>,
    ast: AST,
}

impl Expression {
    fn new(engine: &ExprEngine, ast: AST) -> Result<Self> {
        let result = Self {
            parent: engine.inner.clone(),
            ast,
        };
        result.evaluate("".to_string())?;
        Ok(result)
    }

    pub fn evaluate(&self, value: String) -> Result<String> {
        let mut scope = Scope::new();
        scope.push("value", value);
        Ok(self.parent.eval_ast_with_scope(&mut scope, &self.ast)?)
    }
}

impl ExprEngine {
    pub fn new() -> Self {
        let mut inner = Engine::new_raw();
        inner.register_global_module(StandardPackage::new().as_shared_module());
        inner.set_optimization_level(OptimizationLevel::Full);

        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn compile(&self, expr: String) -> Result<Expression> {
        log::info!("compiling expression: {:?}", &expr);
        Expression::new(
            &self,
            self.inner.compile_expression(&expr).wrap_err(format!(
                "error compiling script expression from {:?}",
                &expr
            ))?,
        )
    }
}
