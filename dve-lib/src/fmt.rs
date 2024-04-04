use crate::{DOp, DataExpr, FrameExpr, Op, Plan, Range, SourceType, Spec, TExpr};
use num_rational::Rational64;

fn pretty_frac(n: Rational64) -> String {
    if *n.denom() == 1 {
        format!("{}", n.numer())
    } else {
        format!("{}/{}", n.numer(), n.denom())
    }
}

impl std::fmt::Display for Plan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Plan({})", self.op)
    }
}

impl std::fmt::Display for DOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.deps.is_empty() {
            write!(f, "{}", self.op)
        } else {
            write!(f, "{} after [", self.op)?;
            let mut comma = false;
            for dep in &self.deps {
                if comma {
                    write!(f, ", ")?;
                }
                write!(f, "{}", dep)?;
                comma = true;
            }
            write!(f, "]")
        }
    }
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::FFmpegClip {
                input,
                range,
                out,
                method,
                codec,
            } => {
                write!(
                    f,
                    "FFmpegClip({:?} clip on {} from {} to {})",
                    method,
                    input,
                    crate::ffmpeg_time(&range.start, false),
                    crate::ffmpeg_time(&range.end, false)
                )
            }
            Op::FFmpegConcat { inputs, out } => {
                write!(f, "FFmpegConcat(...)")
            }
            _ => write!(f, "{:?}", self),
        }
    }
}

impl std::fmt::Display for TExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TExpr::Const(t) => write!(f, "{}", pretty_frac(*t)),
            TExpr::T => write!(f, "t"),
            TExpr::Add(texpr, t) => write!(f, "({} + {})", texpr, t),
            TExpr::Sub(texpr, t) => write!(f, "({} - {})", texpr, t),
            TExpr::Mul(texpr, t) => write!(f, "({} * {})", texpr, t),
        }
    }
}

impl std::fmt::Display for FrameExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameExpr::MatchT(cases) => {
                write!(f, "match{{")?;
                for (case_domain, case_expr) in cases {
                    write!(f, "t in {} => {}, ", case_domain, case_expr)?;
                }
                write!(f, "}}")
            }
            FrameExpr::F2fFunction {
                func,
                sources,
                args,
            } => {
                write!(f, "{:?}(", func)?;
                let mut comma = false;
                for source in sources {
                    if comma {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", source)?;
                    comma = true;
                }
                for arg in args {
                    if comma {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", arg)?;
                    comma = true;
                }
                write!(f, ")")
            }
            FrameExpr::SourceFunction {
                func,
                source,
                t,
                args,
            } => {
                debug_assert_eq!(func, &SourceType::ReadFrame);
                debug_assert!(args.is_empty());
                write!(f, "vid<{}>[{}]", source, t)
            }
        }
    }
}

/*
pub enum DataExpr {
    ConstNum(Rational64),
    ConstStr(String),
    ConstBool(bool),
    ArrayIdx(String, Box<TExpr>),
}
*/

impl std::fmt::Display for DataExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataExpr::ConstNum(n) => write!(f, "{}", pretty_frac(*n)),
            DataExpr::ConstStr(s) => write!(f, "\"{}\"", s),
            DataExpr::ConstBool(b) => write!(f, "{}", b),
            DataExpr::ArrayIdx(name, idx) => write!(f, "{}[{}]", name, idx),
        }
    }
}

impl std::fmt::Display for Spec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Iter={};Render={})", self.iter, self.render,)
    }
}

impl std::fmt::Display for Range {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Range({}, {}, {})",
            pretty_frac(self.start),
            pretty_frac(self.end),
            pretty_frac(self.step),
        )
    }
}
