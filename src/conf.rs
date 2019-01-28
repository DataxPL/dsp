use structopt::StructOpt;

use std::path::PathBuf;

arg_enum! {
    #[derive(Clone, Copy)]
    pub enum Compression {
        None = 0xff,
        LZ4 = 0x1,
    }
}

#[derive(StructOpt)]
#[structopt(name = "dsp")]
pub struct Conf {
    #[structopt(short, long)]
    pub dimensions: Vec<String>,

    #[structopt(short, long)]
    pub metrics: Vec<String>,

    #[structopt(short, long, default_value = "output", parse(from_os_str))]
    pub output: PathBuf,

    #[structopt(short, long, default_value = "none",
        raw(
            possible_values = "&Compression::variants()",
            case_insensitive = "true",
        ),
    )]
    pub compression: Compression,

    #[structopt(short, long, raw(default_value = "&numcpus"))]
    pub threads: usize,

    #[structopt(name = "FILE")]
    pub file: String,
}

lazy_static! {
    static ref numcpus: String = num_cpus::get().to_string();
    pub static ref vals: Conf = Conf::from_args();
}
