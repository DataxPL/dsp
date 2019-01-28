#![feature(test)]

extern crate test;
extern crate dsp;
#[macro_use]
extern crate lazy_static;
extern crate rand;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use test::Bencher;
    use dsp::ValVec;

    use rand::{thread_rng, Rng};

    static NS: usize = 2000;
    static N: usize = 1000000;

    lazy_static! {
        static ref STRINGS: Vec<String> = {
            let mut strings = Vec::new();
            for _i in 1..=NS {
                strings.push(thread_rng()
                             .gen_ascii_chars().take(10).collect::<String>());
            }
            strings
        };
    }

    #[bench]
    fn bench_insert_one(b: &mut Bencher) {
        let mut data: HashMap<String, ValVec> = HashMap::new();

        b.iter(|| {
            for i in 1..N {
                let key = &STRINGS[i % NS];
                if !data.contains_key(key) {
                    data.insert(key.to_string(), ValVec::Integer(Vec::new()));
                }
                let vo = data.get_mut(key).unwrap();
                vo.push_i(i as i64);
            }
        });
    }

    #[bench]
    fn bench_insert_two(b: &mut Bencher) {
        let mut data: HashMap<String, ValVec> = HashMap::new();

        b.iter(|| {
            for i in 1..N {
                let key = &STRINGS[i % NS];
                data
                    .entry(key.to_string())
                    .or_insert(ValVec::Integer(Vec::new()))
                    .push_i(i as i64);
            }
        });
    }
}
