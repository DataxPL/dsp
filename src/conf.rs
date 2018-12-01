use structopt::StructOpt;

arg_enum! {
    #[derive(Clone, Copy)]
    pub enum Compression {
        None = 0xff,
        LZ4 = 0x1,
    }
}

#[derive(StructOpt)]
#[structopt(name = "ds")]
pub struct Conf {
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
