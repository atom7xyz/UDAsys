use noise::{NoiseFn, Fbm, Perlin};
use rand::rng;
use rand_distr::{Distribution, Normal};

fn main() {
    example();
}

fn example() {
    let fbm = Fbm::<Perlin>::new(42);
    let jitter = Normal::new(0.0, 0.5).unwrap();
    let mut rng = rng();

    let base_alt = 1500.0;
    let swing = 120.0;
    let dt = 0.1;

    for i in 0..50 {
        let t = i as f64 * dt;
        let trend = fbm.get([t, 0.0]) * swing;
        let alt = base_alt + trend + jitter.sample(&mut rng);
        println!("t={t:.1}s  alt={alt:.2}m");
    }
}
