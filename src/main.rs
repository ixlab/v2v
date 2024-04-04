use clap::{Parser, Subcommand};
use log::*;
use std::collections::BTreeMap;
use std::time::Instant;

use dve_lib::*;

include!("datasets.rs");

#[derive(serde::Serialize)]
struct DataOut {
    measures: Vec<Measure>,
}

#[derive(serde::Serialize)]
struct Measure {
    spec_name: String,
    opt_level: String,
    spec: String,
    run_n: usize,
    plan: String,
    time: f64,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    cmd: ArgCmd,
}

#[derive(Parser, Debug, Clone, clap::ValueEnum, PartialEq)]
enum Dataset {
    Tos,
    Kabr,
    Custom,
}

#[derive(Parser, Debug)]
struct BenchmarkCmd {
    #[clap(long)]
    datastore: String,

    #[clap(long)]
    dataset: Dataset,

    #[clap(long, default_value = "datalog.json")]
    datalog: String,

    #[clap(long, default_value = "1")]
    warm_ups: usize,

    #[clap(long, default_value = "5")]
    runs: usize,

    #[clap(long)]
    opt_only: bool,
}

#[derive(Parser, Debug, Clone, clap::ValueEnum, PartialEq)]
enum OptimizerLevel {
    Unopt,
    Heuristic,
}

#[derive(Parser, Debug)]
struct PlanCmd {
    #[clap(long)]
    datastore: String,

    #[clap(long)]
    spec: String,

    #[clap(long)]
    opt_level: OptimizerLevel,

    #[clap(long)]
    run: bool,
}

#[derive(Parser, Debug)]
struct AddVideoCmd {
    #[clap(long)]
    datastore: String,

    #[clap(long)]
    name: String,

    #[clap(long)]
    parent_video: Option<String>,

    #[clap(long)]
    video_path: String,

    #[clap(long)]
    ffprobe_json: String,
}

#[derive(Subcommand, Debug)]
enum ArgCmd {
    Benchmark(BenchmarkCmd),
    Plan(PlanCmd),
    AddVideo(AddVideoCmd),
}

fn cmd_benchmark(cmd: BenchmarkCmd) {
    debug!("Loading datastore...");
    let datastore = Datastore::load(std::path::Path::new(&cmd.datastore));
    debug!("Loaded datastore!");

    let mut eval_specs = vec![];
    if cmd.dataset == Dataset::Custom {
        for entry in std::fs::read_dir("custom_specs").unwrap() {
            let entry = entry.unwrap();
            let spec_name = entry.file_name().into_string().unwrap();
            let spec = std::fs::read_to_string(format!("custom_specs/{}", spec_name)).unwrap();
            let spec: Spec = serde_json::from_str(&spec).unwrap();
            eval_specs.push((spec_name.to_string(), spec));
        }
    } else {
        for spec_name in ["S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8"] {
            let spec = std::fs::read_to_string(format!("specs/{}.json", spec_name)).unwrap();
            let spec: Spec = serde_json::from_str(&spec).unwrap();
            eval_specs.push((spec_name.to_string(), spec));
        }
    }

    let eval_specs = eval_specs;

    struct OptimizationLevel {
        name: &'static str,
        exec_fn: Box<dyn Fn(&Spec, &Datastore)>,
        plan_fn: Box<dyn Fn(&Spec, &Datastore) -> String>,
    }

    let eval_query = |query_name: &str,
                      queries: &[Spec],
                      opt_levels: &[OptimizationLevel],
                      datastore: &Datastore|
     -> Vec<Measure> {
        let mut out = Vec::new();

        for opt_level in opt_levels {
            for query in queries {
                let spec_string = format!("{}", query);
                let plan_string = (opt_level.plan_fn)(query, datastore);

                for _i in 0..cmd.warm_ups {
                    (opt_level.exec_fn)(query, datastore);
                    cleanup();
                }
                for run_n in 0..cmd.runs {
                    let start = Instant::now();
                    (opt_level.exec_fn)(query, datastore);
                    let duration = start.elapsed();

                    out.push(Measure {
                        spec_name: query_name.to_string(),
                        opt_level: opt_level.name.to_string(),
                        spec: spec_string.clone(),
                        run_n,
                        plan: plan_string.clone(),
                        time: duration.as_secs_f64(),
                    });
                    cleanup();
                }
            }
        }

        out
    };

    let mut datalog = DataOut { measures: vec![] };

    for (eval_spec_name, query) in eval_specs {
        info!("Evaluating spec {eval_spec_name}");

        let opt_only = cmd.opt_only;

        let oneshot_optimizers: Vec<OptimizationLevel> = vec![
            OptimizationLevel {
                name: "Unoptimized",
                exec_fn: Box::new(move |query: &Spec, datastore: &Datastore| {
                    let unopt_plan = plan_query(query, datastore);
                    if !opt_only {
                        unopt_plan.run(false);
                    }
                }),
                plan_fn: Box::new(|query: &Spec, datastore: &Datastore| {
                    let unopt_plan = plan_query(query, datastore);
                    format!("{:?}", unopt_plan)
                }),
            },
            OptimizationLevel {
                name: "Heuristic",
                exec_fn: Box::new(move |query: &Spec, datastore: &Datastore| {
                    let unopt_plan = plan_query(query, datastore);
                    let huristic_optimized_plan = unopt_plan.clone().optimize_heuristic(datastore);
                    if !opt_only {
                        huristic_optimized_plan.run(true);
                    }
                }),
                plan_fn: Box::new(|query: &Spec, datastore: &Datastore| {
                    let unopt_plan = plan_query(query, datastore);
                    let huristic_optimized_plan = unopt_plan.clone().optimize_heuristic(datastore);
                    format!("{:?}", huristic_optimized_plan)
                }),
            },
        ];

        let eval_query_set: Vec<Spec> = match cmd.dataset {
            Dataset::Tos => vec![query.clone()],
            Dataset::Custom => vec![query.clone()],
            Dataset::Kabr => KABR_IDS
                .iter()
                .map(|kabr_id| {
                    let mut q = query.clone();
                    q.set_all_sources(&format!("videos/{}.MP4", kabr_id));
                    q
                })
                .collect(),
        };

        {
            let mut measures = eval_query(
                &eval_spec_name,
                &eval_query_set,
                oneshot_optimizers.as_slice(),
                &datastore,
            );
            datalog.measures.append(&mut measures);

            let datalog_text = serde_json::to_string_pretty(&datalog).unwrap();
            std::fs::write(&cmd.datalog, datalog_text).unwrap();
        }
    }

    info!("Finished benchmarking!");
}

fn cmd_plan(cmd: PlanCmd) {
    debug!("Loading datastore...");
    let datastore = Datastore::load(std::path::Path::new(&cmd.datastore));
    debug!("Loaded datastore!");

    let spec = std::fs::read_to_string(cmd.spec).unwrap();
    let spec: Spec = serde_json::from_str(&spec).unwrap();

    let plan = plan_query(&spec, &datastore);

    let opt_plan = match cmd.opt_level {
        OptimizerLevel::Unopt => plan,
        OptimizerLevel::Heuristic => plan.optimize_heuristic(&datastore),
    };

    println!("{}", opt_plan);

    if cmd.run {
        opt_plan.run(cmd.opt_level != OptimizerLevel::Unopt);
    }
}

fn cmd_add_video(cmd: AddVideoCmd) {
    let mut datastore = if std::path::Path::new(&cmd.datastore).exists() {
        debug!("Loading datastore...");
        let d = Datastore::load(std::path::Path::new(&cmd.datastore));
        debug!("Loaded datastore!");
        d
    } else {
        debug!("No datastore found, creating new one...");
        Datastore {
            videos: BTreeMap::new(),
            tree_idxs: BTreeMap::new(),
        }
    };

    let video_source = VideoSource {
        name: cmd.name.clone(),
        path: cmd.video_path.clone(),
        ffprobe_path: cmd.ffprobe_json.clone(),
    };

    let (baseline_path, parent) = match cmd.parent_video {
        Some(root_video) => {
            let parent = datastore.videos.get(&root_video).unwrap();
            (parent.path.clone(), Some(root_video))
        }
        None => (video_source.path.clone(), None),
    };

    debug!("Profiling video...");
    datastore.add_new_video(&video_source);
    debug!("Profiling video done!");

    debug!("Saving datastore...");
    datastore.save(std::path::Path::new(&cmd.datastore));
    debug!("Saved datastore!");
}

fn main() {
    pretty_env_logger::init();
    let args = Args::parse();
    trace!("Args: {:#?}", args);

    match args.cmd {
        ArgCmd::Benchmark(cmd) => cmd_benchmark(cmd),
        ArgCmd::Plan(cmd) => cmd_plan(cmd),
        ArgCmd::AddVideo(cmd) => cmd_add_video(cmd),
    }
}
