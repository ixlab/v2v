use log::*;
use num_rational::Rational64;
use num_traits::cast::ToPrimitive;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use uuid::Uuid;

mod fmt;

const TARGET_WIDTH: usize = 1280;
const TARGET_HEIGHT: usize = 720;

#[derive(Serialize, Deserialize, Debug)]
pub struct Datastore {
    pub videos: BTreeMap<String, Video>,
    pub tree_idxs: BTreeMap<String, (BTreeSet<Rational64>, BTreeSet<Rational64>)>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Video {
    pub path: String,
    pub ffprobe_path: String,
    pub range: Range,
    pub gops: Vec<SourceGopBound>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Codec {
    H264,
    VP9,
}

pub struct VideoSource {
    pub name: String,
    pub path: String,
    pub ffprobe_path: String,
}

pub enum ClipSide {
    Starting,
    Ending,
}

impl Datastore {
    pub fn save(&self, file_path: &std::path::Path) {
        use std::io::Write;
        let serialized = serde_json::to_string(self).expect("Failed to serialize data");
        let mut file = std::fs::File::create(file_path).unwrap();
        file.write_all(serialized.as_bytes()).unwrap();
    }

    pub fn load(file_path: &std::path::Path) -> Self {
        let file = std::fs::File::open(file_path).unwrap();
        let reader = std::io::BufReader::new(file);
        serde_json::from_reader(reader).unwrap()
    }

    fn path_to_vid_key(&self, path: &str) -> String {
        self.videos
            .iter()
            .filter(|x| x.1.path == path)
            .take(1)
            .next()
            .expect(format!("Failed to find video from path \"{}\"", path).as_str())
            .0
            .to_string()
    }

    fn vid_key_to_path(&self, key: &str) -> String {
        self.videos
            .get(key)
            .expect("Failed to find video from key")
            .path
            .to_string()
    }

    pub fn add_new_video(&mut self, source: &VideoSource) {
        cleanup();

        if self.videos.contains_key(&source.name) {
            info!(
                "Skipping video {} since it's already in the datastore",
                source.name
            );
            return;
        }

        let (range, gops, _codec) = load_meta(&source.ffprobe_path);

        self.videos.insert(
            source.name.to_string(),
            Video {
                path: source.path.to_string(),
                ffprobe_path: source.ffprobe_path.to_string(),
                range,
                gops,
            },
        );

        cleanup();
    }

    pub fn add_new_video_tree(&mut self, root: &VideoSource, children: &[VideoSource]) {
        self.add_new_video(root);

        for child in children {
            self.add_new_video(child);
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Range {
    pub start: Rational64,
    pub end: Rational64,
    pub step: Rational64,
}

impl Range {
    fn len(&self) -> i64 {
        let cnt = (self.end - self.start) / self.step;
        assert!(*cnt.denom() == 1);
        *cnt.numer() + 1 // inclusive range, so add one
    }

    fn split_at(&self, split_pt: Rational64) -> (Range, Range) {
        let left = Range {
            start: self.start,
            end: split_pt,
            step: self.step,
        };
        let right = Range {
            start: split_pt + self.step,
            end: self.end,
            step: self.step,
        };
        assert!(left.start <= left.end);
        // if !(right.start <= right.end) {
        //     dbg!(self, split_pt, &left, &right);
        // }
        // assert!(right.start <= right.end);
        assert!(left.len() + right.len() == self.len());
        (left, right)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataExpr {
    ConstNum(Rational64),
    ConstStr(String),
    ConstBool(bool),
    ArrayIdx(String, Box<TExpr>),
}

impl DataExpr {
    fn unwrap_const_str(&self) -> String {
        match self {
            DataExpr::ConstStr(s) => s.clone(),
            _ => panic!("unwrap_const_str"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TExpr {
    Const(Rational64),
    T,
    Add(Box<TExpr>, Rational64),
    Sub(Box<TExpr>, Rational64),
    Mul(Box<TExpr>, Rational64),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SourceType {
    ReadFrame,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum F2FType {
    Quadrents,
    Filter,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FrameExpr {
    MatchT(Vec<(Range, Box<FrameExpr>)>),
    F2fFunction {
        func: F2FType,
        sources: Vec<FrameExpr>,
        args: Vec<DataExpr>,
    },
    SourceFunction {
        func: SourceType,
        source: String,
        t: TExpr,
        args: Vec<DataExpr>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Spec {
    pub iter: Range,
    pub render: FrameExpr,
    pub output: String,
}

#[derive(Debug)]
pub enum ArraySource {
    DatabaseVideo,
    QueryArray,
}

impl Spec {
    fn range_deps(&self) -> Vec<(String, ArraySource, Range)> {
        self.render.range_deps(&self.iter)
    }

    fn flatten_matches(&self) -> Vec<(Range, FrameExpr)> {
        self.render.flatten_matches(&self.iter)
    }

    pub fn set_all_sources(&mut self, source: &str) {
        self.render.set_all_sources(source);
    }
}

impl FrameExpr {
    fn range_deps(self: &FrameExpr, domain: &Range) -> Vec<(String, ArraySource, Range)> {
        let mut out = vec![];

        match self {
            FrameExpr::MatchT(cases) => {
                for (case_domain, expr) in cases {
                    out.extend(expr.range_deps(case_domain));
                }
            }
            FrameExpr::F2fFunction {
                sources: source,
                args,
                ..
            } => {
                for source in source {
                    out.extend(source.range_deps(domain));
                }

                for arg in args {
                    if let DataExpr::ArrayIdx(name, texpr) = arg {
                        out.push((name.clone(), ArraySource::QueryArray, texpr.range(domain)));
                    }
                }
            }
            FrameExpr::SourceFunction {
                source, t, args, ..
            } => {
                out.push((source.clone(), ArraySource::DatabaseVideo, t.range(domain)));

                for arg in args {
                    if let DataExpr::ArrayIdx(name, texpr) = arg {
                        out.push((name.clone(), ArraySource::QueryArray, texpr.range(domain)));
                    }
                }
            }
        }

        out
    }

    fn flatten_matches(&self, domain: &Range) -> Vec<(Range, Self)> {
        let mut out = vec![];
        match self {
            FrameExpr::MatchT(cases) => {
                for (case_range, case_expr) in cases {
                    out.extend(case_expr.flatten_matches(case_range));
                }
            }
            FrameExpr::F2fFunction {
                func,
                sources,
                args,
            } => {
                // We're not flattening through a f2f here since it's really hard logic
                out.push((
                    domain.clone(),
                    FrameExpr::F2fFunction {
                        func: func.clone(),
                        sources: sources.iter().map(|x| (*x).clone()).collect(),
                        args: args.clone(),
                    },
                ));
            }
            FrameExpr::SourceFunction {
                func,
                source,
                t,
                args,
            } => {
                out.push((
                    domain.clone(),
                    FrameExpr::SourceFunction {
                        func: func.clone(),
                        source: source.clone(),
                        t: t.clone(),
                        args: args.clone(),
                    },
                ));
            }
        }
        out
    }

    fn set_all_sources(&mut self, new_source: &str) {
        match self {
            FrameExpr::MatchT(cases) => {
                for (_, expr) in cases {
                    expr.set_all_sources(new_source);
                }
            }
            FrameExpr::F2fFunction { sources, .. } => {
                for source in sources {
                    source.set_all_sources(new_source);
                }
            }
            FrameExpr::SourceFunction { source: s, .. } => {
                *s = new_source.to_string();
            }
        }
    }
}

impl TExpr {
    fn range(self: &TExpr, domain: &Range) -> Range {
        match self {
            TExpr::Const(t) => Range {
                start: *t,
                end: *t + Rational64::new(1, 1),
                step: Rational64::new(1, 1),
            },
            TExpr::T => domain.clone(),
            TExpr::Add(texpr, t) => {
                let mut range = texpr.range(domain);
                range.start += t;
                range.end += t;
                range
            }
            TExpr::Sub(texpr, t) => {
                let mut range = texpr.range(domain);
                range.start -= t;
                range.end -= t;
                range
            }
            TExpr::Mul(texpr, t) => {
                let mut range = texpr.range(domain);
                range.start *= t;
                range.end *= t;
                range.step *= t;
                range
            }
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
enum FFmpegClipMethod {
    Transcode,
    StreamCopy,
}

#[derive(Debug, Clone)]
enum Op {
    FFmpegClip {
        input: String,
        range: Range,
        out: String,
        method: FFmpegClipMethod,
        codec: Codec,
    },
    FFmpegConcat {
        inputs: Vec<String>,
        out: String,
    },
    FFmpegFilter {
        inputs: Vec<(String, Option<Range>)>,
        filter: String,
        complex: bool,
        approx: bool,
        out: String,
    },
}

fn ffmpeg_time(t: &Rational64, duration: bool) -> String {
    // todo: don't hard-code 24FPS
    let duration_epsilon = if duration { 0.5 / 24.0 } else { 0.0 };
    format!(
        "{:0.6}",
        (*t.numer() as f64) / (*t.denom() as f64) + if duration { duration_epsilon } else { 0.0 }
    )
}

impl Op {
    fn run(&self) {
        match self {
            Op::FFmpegClip {
                input,
                range,
                out,
                method,
                codec,
            } => {
                let mut cmd = std::process::Command::new("ffmpeg");
                cmd.arg("-hide_banner");
                cmd.arg("-loglevel").arg("error");

                cmd.arg("-ss").arg(ffmpeg_time(&range.start, false));
                cmd.arg("-i").arg(input);
                cmd.arg("-t")
                    .arg(ffmpeg_time(&(range.end - range.start), true));

                match method {
                    FFmpegClipMethod::StreamCopy => {
                        cmd.arg("-c:v").arg("copy");
                        cmd.arg("-c:a").arg("copy");
                    }
                    FFmpegClipMethod::Transcode => {
                        match codec {
                            Codec::H264 => {
                                cmd.arg("-c:v").arg("libx264");
                                cmd.arg("-preset").arg("ultrafast");
                            }
                            Codec::VP9 => {
                                cmd.arg("-c:v").arg("libvpx-vp9");
                                cmd.arg("-deadline").arg("realtime");
                                cmd.arg("-speed").arg("8");
                            }
                        }
                        cmd.arg("-vf")
                            .arg(format!("scale={TARGET_WIDTH}:{TARGET_HEIGHT}"));
                    }
                }

                cmd.arg(out);
                cmd.arg("-y");

                info!("{cmd:?}",);

                // run cmd
                let status = cmd.status().expect("failed to execute process");
                assert!(status.success());
            }
            Op::FFmpegConcat { inputs: input, out } => {
                let job_file_path = format!("/scratch/tmp_{}.txt", Uuid::new_v4());
                let job_file_content = input
                    .iter()
                    .map(|f| format!("file '{f}'",))
                    .collect::<Vec<String>>()
                    .join("\n");
                fs::write(&job_file_path, job_file_content).expect("Unable to write to file");

                let mut cmd = std::process::Command::new("ffmpeg");
                cmd.arg("-hide_banner");
                cmd.arg("-loglevel").arg("error");

                cmd.arg("-f").arg("concat");
                cmd.arg("-safe").arg("0");
                cmd.arg("-i").arg(job_file_path);
                cmd.arg("-c").arg("copy");

                cmd.arg(out);
                cmd.arg("-y");

                info!("{cmd:?}",);
                let status = cmd.status().expect("failed to execute process");
                assert!(status.success());
            }
            Op::FFmpegFilter {
                inputs,
                filter,
                complex,
                approx,
                out,
            } => {
                let mut cmd = std::process::Command::new("ffmpeg");
                cmd.arg("-hide_banner");
                cmd.arg("-loglevel").arg("error");

                for (input, input_range) in inputs {
                    if let Some(input_range) = input_range {
                        cmd.arg("-ss").arg(ffmpeg_time(&input_range.start, false));
                    }
                    cmd.arg("-i").arg(input);
                }

                if let Some(input_range) = &inputs[0].1 {
                    cmd.arg("-t")
                        .arg(ffmpeg_time(&(input_range.end - input_range.start), true));
                }

                if *complex {
                    // complex filters should handle their own resolution

                    if *approx {
                        cmd.arg("-filter_complex").arg(filter.replace(
                            &format!("setpts=PTS-STARTPTS, scale={TARGET_WIDTH}x{TARGET_HEIGHT}"),
                            &format!(
                                "setpts=PTS-STARTPTS, fps=12, scale={TARGET_WIDTH}x{TARGET_HEIGHT}"
                            ),
                        ));
                    } else {
                        cmd.arg("-filter_complex").arg(filter);
                    }
                } else if *approx {
                    cmd.arg("-vf").arg(format!(
                        "fps=12,scale={TARGET_WIDTH}:{TARGET_HEIGHT},{filter}"
                    ));
                } else {
                    cmd.arg("-vf")
                        .arg(format!("scale={TARGET_WIDTH}:{TARGET_HEIGHT},{filter}"));
                }

                cmd.arg("-c:v").arg("libx264");
                cmd.arg("-preset").arg("ultrafast");

                cmd.arg(out);
                cmd.arg("-y");

                info!("{cmd:?}",);
                let status = cmd.status().expect("failed to execute process");
                assert!(status.success());
            }
        }
    }
}

/// Dependency-tracked Op
#[derive(Debug, Clone)]
struct DOp {
    op: Op,
    deps: Vec<DOp>,
}

impl DOp {
    fn run(&self, parallel: bool) {
        if parallel {
            self.deps.par_iter().for_each(|dep| dep.run(parallel));
        } else {
            for dep in &self.deps {
                dep.run(parallel);
            }
        }

        self.op.run();
    }

    fn optimize_shard_filters(self) -> DOp {
        const SHARD_FRAMES: i64 = 300;

        match self.op {
            Op::FFmpegFilter {
                inputs,
                filter,
                complex,
                out,
                approx,
            } if inputs[0].1.is_some() && inputs[0].1.clone().unwrap().len() > SHARD_FRAMES => {
                let mut out_deps = vec![];
                let mut out_inputs = vec![];

                let mut active_ranges = inputs
                    .iter()
                    .map(|x| x.1.clone().unwrap())
                    .collect::<Vec<_>>();

                for i in 0..active_ranges.len() {
                    assert!(active_ranges[i].start <= active_ranges[i].end);
                    assert!(active_ranges[0].len() == active_ranges[i].len());
                }

                let shard_duration: Rational64 =
                    Rational64::new(SHARD_FRAMES, 1) * active_ranges[0].step;

                loop {
                    let shard_pts: Vec<Rational64> = active_ranges
                        .iter()
                        .map(|x| x.start + shard_duration)
                        .collect();

                    let splits = active_ranges
                        .iter()
                        .enumerate()
                        .map(|(i, r)| r.split_at(shard_pts[i]))
                        .collect::<Vec<_>>();

                    let shard_name = format!("/scratch/tmp_{}.mp4", Uuid::new_v4());

                    let mut new_inputs = vec![];
                    for i in 0..inputs.len() {
                        assert!(splits[i].0.start <= splits[i].0.end);
                        new_inputs.push((inputs[i].0.clone(), Some(splits[i].0.clone())));
                    }

                    let shard = Op::FFmpegFilter {
                        inputs: new_inputs,
                        filter: filter.clone(),
                        complex,
                        out: shard_name.clone(),
                        approx,
                    };
                    out_deps.push(DOp {
                        op: shard,
                        deps: self.deps.to_vec(),
                    });
                    out_inputs.push(shard_name);

                    if active_ranges[0].start + shard_duration == active_ranges[0].end {
                        break;
                    }

                    for i in 0..inputs.len() {
                        active_ranges[i].start = shard_pts[i];
                    }

                    if active_ranges[0].start + shard_duration > active_ranges[0].end {
                        let shard_name = format!("/scratch/tmp_{}.mp4", Uuid::new_v4());
                        let mut new_inputs = vec![];
                        for i in 0..inputs.len() {
                            assert!(splits[i].1.start <= splits[i].1.end);
                            new_inputs.push((inputs[i].0.clone(), Some(splits[i].1.clone())));
                        }
                        let shard = Op::FFmpegFilter {
                            inputs: new_inputs,
                            filter: filter.clone(),
                            complex,
                            out: shard_name.clone(),
                            approx,
                        };
                        out_deps.push(DOp {
                            op: shard,
                            deps: self.deps.clone(),
                        });
                        out_inputs.push(shard_name);

                        break;
                    }
                }

                DOp {
                    op: Op::FFmpegConcat {
                        inputs: out_inputs,
                        out,
                    },
                    deps: out_deps,
                }
            }
            Op::FFmpegConcat { inputs, out } => {
                let mut new_deps = vec![];
                for dep in self.deps {
                    new_deps.push(dep.optimize_shard_filters());
                }
                DOp {
                    op: Op::FFmpegConcat { inputs, out },
                    deps: new_deps,
                }
            }
            _ => self,
        }
    }

    fn optimize_seek_pullup(self) -> DOp {
        match self.op {
            Op::FFmpegConcat { inputs: input, out } => DOp {
                op: Op::FFmpegConcat { inputs: input, out },
                deps: self
                    .deps
                    .into_iter()
                    .map(|x| x.optimize_seek_pullup())
                    .collect(),
            },
            Op::FFmpegClip {
                input,
                range,
                out,
                method,
                codec,
            } => DOp {
                op: Op::FFmpegClip {
                    input,
                    range,
                    out,
                    method,
                    codec,
                },
                deps: self
                    .deps
                    .into_iter()
                    .map(|x| x.optimize_seek_pullup())
                    .collect(),
            },
            Op::FFmpegFilter {
                mut inputs,
                filter,
                complex,
                out,
                approx,
            } => {
                let mut out_deps = vec![];
                for (i, dep) in self.deps.into_iter().enumerate() {
                    match dep.op {
                        Op::FFmpegClip {
                            input,
                            range,
                            out,
                            method,
                            codec,
                        } => {
                            if method == FFmpegClipMethod::Transcode && dep.deps.is_empty() {
                                inputs[i].0 = input;
                                inputs[i].1 = Some(range);
                            } else {
                                out_deps.push(DOp {
                                    op: Op::FFmpegClip {
                                        input,
                                        range,
                                        out,
                                        method,
                                        codec,
                                    },
                                    deps: dep.deps,
                                });
                            }
                        }
                        _ => {
                            out_deps.push(dep.optimize_seek_pullup());
                        }
                    }
                }

                DOp {
                    op: Op::FFmpegFilter {
                        inputs,
                        filter,
                        complex,
                        out,
                        approx,
                    },
                    deps: out_deps,
                }
            }
        }
    }

    fn optimize_smart_cut(self, datastore: &Datastore) -> DOp {
        match self.op {
            Op::FFmpegClip {
                input,
                range,
                out,
                method,
                codec,
            } => {
                let iframes: Vec<Rational64> = datastore
                    .videos
                    .get(&datastore.path_to_vid_key(&input))
                    .unwrap()
                    .gops
                    .iter()
                    .map(|g| g.start)
                    .filter(|t| *t >= range.start && *t <= range.end)
                    .collect();

                // println!("iframes: {:?}", iframes);

                if iframes.len() >= 2
                    && method == FFmpegClipMethod::Transcode
                    && self.deps.is_empty()
                {
                    let head_name = format!("/scratch/tmp_{}.mp4", Uuid::new_v4());
                    let head = Op::FFmpegClip {
                        input: input.clone(),
                        range: Range {
                            start: range.start,
                            end: iframes[0],
                            step: range.step,
                        },
                        out: head_name.clone(),
                        method: FFmpegClipMethod::Transcode,
                        codec,
                    };

                    let body_name = format!("/scratch/tmp_{}.mp4", Uuid::new_v4());
                    let body = Op::FFmpegClip {
                        input: input.clone(),
                        range: Range {
                            start: iframes[0],
                            end: iframes[iframes.len() - 1],
                            step: range.step,
                        },
                        out: body_name.clone(),
                        method: FFmpegClipMethod::StreamCopy,
                        codec,
                    };

                    let tail_name = format!("/scratch/tmp_{}.mp4", Uuid::new_v4());
                    let tail = Op::FFmpegClip {
                        input: input.clone(),
                        range: Range {
                            start: iframes[iframes.len() - 1],
                            end: range.end,
                            step: range.step,
                        },
                        out: tail_name.clone(),
                        method: FFmpegClipMethod::Transcode,
                        codec,
                    };

                    let mut concat_inputs = vec![];
                    let mut concat_deps = vec![];

                    // Don't add a zero time head if we're starting on a keyframe
                    if range.start < iframes[0] {
                        concat_inputs.push(head_name);
                        concat_deps.push(DOp {
                            op: head,
                            deps: vec![],
                        });
                    }

                    concat_inputs.push(body_name);
                    concat_deps.push(DOp {
                        op: body,
                        deps: vec![],
                    });

                    // Don't add a zero time tail if we're ending on a keyframe
                    if iframes[iframes.len() - 1] < range.end {
                        concat_inputs.push(tail_name);
                        concat_deps.push(DOp {
                            op: tail,
                            deps: vec![],
                        });
                    }

                    let concat = Op::FFmpegConcat {
                        inputs: concat_inputs,
                        out,
                    };

                    DOp {
                        op: concat,
                        deps: concat_deps,
                    }
                } else {
                    DOp {
                        op: Op::FFmpegClip {
                            input,
                            range,
                            out,
                            method,
                            codec,
                        },
                        deps: self.deps,
                    }
                }
            }
            Op::FFmpegConcat { inputs: input, out } => DOp {
                op: Op::FFmpegConcat { inputs: input, out },
                deps: self
                    .deps
                    .iter()
                    .map(|x| x.clone().optimize_smart_cut(datastore))
                    .collect(),
            },
            Op::FFmpegFilter {
                inputs,
                filter,
                complex,
                out,
                approx,
            } => DOp {
                op: Op::FFmpegFilter {
                    inputs,
                    filter,
                    complex,
                    out,
                    approx,
                },
                deps: self.deps,
            },
        }
    }

    fn optimize_concat_squash(self) -> DOp {
        match self.op {
            Op::FFmpegConcat { inputs, out } => {
                let concat_deps: Vec<DOp> = self
                    .deps
                    .iter()
                    .filter(|x| matches!(x.op, Op::FFmpegConcat { .. }))
                    .cloned()
                    .collect();

                let mut new_deps: Vec<DOp> = self
                    .deps
                    .into_iter()
                    .filter(|x| !matches!(x.op, Op::FFmpegConcat { .. }))
                    .collect();

                let mut new_inputs = vec![];

                for input in inputs {
                    let source_concat = concat_deps.iter().find(|x| match &x.op {
                        Op::FFmpegConcat { out: dep_out, .. } => dep_out == &input,
                        _ => false,
                    });

                    if let Some(source_concat) = source_concat {
                        match &source_concat.op {
                            Op::FFmpegConcat {
                                inputs: source_inputs,
                                ..
                            } => {
                                new_inputs.extend(source_inputs.clone());
                                new_deps.extend(source_concat.deps.clone());
                            }
                            _ => unreachable!(),
                        }
                    } else {
                        new_inputs.push(input);
                    }
                }

                DOp {
                    op: Op::FFmpegConcat {
                        inputs: new_inputs,
                        out,
                    },
                    deps: new_deps,
                }
            }
            _ => self,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Plan {
    op: DOp,
}

impl Plan {
    pub fn run(&self, parallel: bool) {
        self.op.run(parallel);
    }

    pub fn optimize_heuristic(&self, datastore: &Datastore) -> Self {
        let mut out = self.clone();
        out.op = out.op.optimize_seek_pullup();
        out.op = out.op.optimize_shard_filters();
        out.op = out.op.optimize_smart_cut(datastore);
        out.op = out.op.optimize_concat_squash();
        out
    }
}

pub fn plan_query(query: &Spec, datastore: &Datastore) -> Plan {
    // println!("Dependency checks:");
    // for (dep_name, dep_type, range) in query.range_deps() {
    //     println!("{:?} {} requires {}", dep_type, dep_name, range);
    // }

    // println!();
    // println!("Flattened query:");
    // for (range, expr) in query.flatten_matches() {
    //     println!("Root clip {}: {:?}", range, expr);
    // }

    // println!();

    let mut root_clips = query.flatten_matches();

    fn plan_clip(datastore: &Datastore, range: &Range, expr: FrameExpr, output: &str) -> DOp {
        match expr {
            FrameExpr::SourceFunction {
                func,
                source,
                t,
                args: _,
            } => match func {
                SourceType::ReadFrame => {
                    let range = t.range(range);

                    DOp {
                        op: Op::FFmpegClip {
                            input: source.clone(),
                            range,
                            out: output.to_string(),
                            method: FFmpegClipMethod::Transcode,
                            codec: Codec::H264,
                        },
                        deps: vec![],
                    }
                }
            },
            FrameExpr::MatchT(_) => unreachable!(),
            FrameExpr::F2fFunction {
                func,
                mut sources,
                args,
            } => match func {
                F2FType::Quadrents => {
                    let quad_outs = [
                        format!("/scratch/tmp_{}.mp4", Uuid::new_v4()),
                        format!("/scratch/tmp_{}.mp4", Uuid::new_v4()),
                        format!("/scratch/tmp_{}.mp4", Uuid::new_v4()),
                        format!("/scratch/tmp_{}.mp4", Uuid::new_v4()),
                    ];

                    let deps = [
                        plan_clip(datastore, range, sources.remove(0), &quad_outs[0]),
                        plan_clip(datastore, range, sources.remove(0), &quad_outs[1]),
                        plan_clip(datastore, range, sources.remove(0), &quad_outs[2]),
                        plan_clip(datastore, range, sources.remove(0), &quad_outs[3]),
                    ];

                    DOp {
                        op: Op::FFmpegFilter {
                            inputs: vec![
                                (quad_outs[0].to_string(), None),
                                (quad_outs[1].to_string(), None),
                                (quad_outs[2].to_string(), None),
                                (quad_outs[3].to_string(), None),
                            ],
                            filter: format!("nullsrc=size={DUB_WIDTH}x{DUB_HEIGHT} [base];[0:v] setpts=PTS-STARTPTS, scale={TARGET_WIDTH}x{TARGET_HEIGHT} [upperleft];[1:v] setpts=PTS-STARTPTS, scale={TARGET_WIDTH}x{TARGET_HEIGHT} [upperright];[2:v] setpts=PTS-STARTPTS, scale={TARGET_WIDTH}x{TARGET_HEIGHT} [lowerleft];[3:v] setpts=PTS-STARTPTS, scale={TARGET_WIDTH}x{TARGET_HEIGHT} [lowerright];[base][upperleft] overlay=shortest=1 [tmp1];[tmp1][upperright] overlay=shortest=1:x={TARGET_WIDTH} [tmp2];[tmp2][lowerleft] overlay=shortest=1:y={TARGET_HEIGHT} [tmp3];[tmp3][lowerright] overlay=shortest=1:x={TARGET_WIDTH}:y={TARGET_HEIGHT}", DUB_WIDTH=TARGET_WIDTH * 2, DUB_HEIGHT=TARGET_HEIGHT * 2),
                            complex: true,
                            out: output.to_string(),
                            approx: false,
                        },
                        deps: deps.into(),
                    }
                }
                F2FType::Filter => {
                    let source_path = format!("/scratch/tmp_{}.mp4", Uuid::new_v4());
                    DOp {
                        op: Op::FFmpegFilter {
                            inputs: vec![(source_path.clone(), None)],
                            complex: false,
                            out: output.to_string(),
                            filter: args[0].unwrap_const_str(),
                            approx: false,
                        },
                        deps: vec![plan_clip(datastore, range, sources.remove(0), &source_path)],
                    }
                }
            },
        }
    }

    if root_clips.len() == 1 {
        let (clip_range, clip_expr) = root_clips.remove(0);
        Plan {
            op: plan_clip(datastore, &clip_range, clip_expr, &query.output),
        }
    } else {
        let mut ops = vec![];
        let mut root_clip_outputs = vec![];
        for (clip_range, clip_expr) in root_clips {
            let clip_output = format!("/scratch/tmp_{}.mp4", Uuid::new_v4());
            let clip_plan = plan_clip(datastore, &clip_range, clip_expr, &clip_output);
            ops.push(clip_plan);
            root_clip_outputs.push(clip_output);
        }
        Plan {
            op: DOp {
                op: Op::FFmpegConcat {
                    inputs: root_clip_outputs,
                    out: query.output.clone(),
                },
                deps: ops,
            },
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SourceGopBound {
    start: Rational64,
    end: Rational64,
}

impl std::fmt::Debug for SourceGopBound {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SourceGopBound {{ start: {:.4}, end: {:.4} }}",
            self.start.to_f64().unwrap(),
            self.end.to_f64().unwrap(),
        )
    }
}

fn load_meta(meta_path: &str) -> (Range, Vec<SourceGopBound>, Codec) {
    let x = std::fs::read_to_string(meta_path).unwrap();
    let v: serde_json::Value = serde_json::from_str(&x).unwrap();
    let y: String = v["streams"][0]["time_base"].as_str().unwrap().to_string();
    let tbn = {
        let mut it = y.split('/');
        let tmp_numer = it.next().unwrap();
        debug_assert!(tmp_numer == "1");
        it.next().unwrap().parse::<i64>().unwrap()
    };

    let mut gop_bounds: Vec<SourceGopBound> = vec![];

    fn get_pts(frame: &serde_json::Value) -> i64 {
        let pts1 = frame["pkt_pts"].as_i64();
        let pts2 = frame["pts"].as_i64();

        // do some sanity checks
        if let Some(pts1) = pts1 {
            if let Some(pts2) = pts2 {
                assert_eq!(pts1, pts2);
            }
        }

        if let Some(pts1) = pts1 {
            pts1
        } else {
            pts2.unwrap()
        }
    }

    // we assume the frames are in order later
    let mut frames = v["frames"].as_array().unwrap().clone();
    frames.sort_by_key(get_pts);

    let mut step = Option::None;
    let mut gop_start: Option<Rational64> = Option::None;
    let mut last_frame: Option<Rational64> = Option::None;

    for frame in &frames {
        let pts = get_pts(frame);
        let pts = Rational64::new(pts, tbn);

        if pts > 0.into() && step.is_none() {
            // the second frame's time is the step
            step = Option::Some(pts);
        }

        if frame["pict_type"].as_str().unwrap() == "I" {
            debug_assert!(frame["key_frame"].as_i64().unwrap() == 1);

            if let Some(prior_gop_start) = gop_start {
                gop_bounds.push(SourceGopBound {
                    start: prior_gop_start,
                    end: last_frame.unwrap(),
                });
            }

            gop_start = Option::Some(pts);
        } else {
            debug_assert!(
                frame["pict_type"].as_str().unwrap() == "P"
                    || frame["pict_type"].as_str().unwrap() == "B"
            );
        }

        last_frame = Option::Some(pts);
    }
    gop_bounds.push(SourceGopBound {
        start: gop_start.unwrap(),
        end: last_frame.unwrap(),
    });

    let codec = match v["streams"][0]["codec_name"]
        .as_str()
        .unwrap()
        .to_string()
        .as_str()
    {
        "h264" => Codec::H264,
        "vp9" => Codec::VP9,
        "av1" => {
            println!("Skipping AV1");
            std::process::exit(0)
        }
        _ => panic!("Unsupported video codec!"),
    };

    (
        Range {
            start: Rational64::new(0, 1),
            end: last_frame.unwrap(),
            step: step.unwrap(),
        },
        gop_bounds,
        codec,
    )
}

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

pub fn cleanup() {
    let status = std::process::Command::new("bash")
        .arg("-c")
        .arg("rm -rf /scratch/*")
        .status()
        .expect("rm command failed to start");
    assert!(status.success());

    let status = std::process::Command::new("sync")
        .status()
        .expect("sync command failed to start");
    assert!(status.success());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
