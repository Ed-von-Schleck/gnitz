use super::{parse_args, parse_workers};
use crate::runtime::MAX_WORKERS;

#[test]
fn parse_workers_accepts_the_valid_range_and_nothing_else() {
    assert_eq!(parse_workers("1"), Ok(1));
    assert_eq!(parse_workers(&MAX_WORKERS.to_string()), Ok(MAX_WORKERS as u32));
    // Above MAX_WORKERS once reached the SAL group writer, which cannot
    // describe a group that wide.
    for bad in ["0", &(MAX_WORKERS + 1).to_string(), "100000", "abc", ""] {
        assert!(parse_workers(bad).is_err(), "{bad:?} is not a worker count");
    }
}

fn argv(args: &[&str]) -> Vec<String> {
    args.iter().map(|a| a.to_string()).collect()
}

#[test]
fn parse_args_rejects_an_unknown_option() {
    let e = parse_args(&argv(&["--tls-lsten=127.0.0.1:0", "/data", "/sock"]), None).err();
    assert_eq!(e.as_deref(), Some("unknown option \"--tls-lsten=127.0.0.1:0\""));
}

#[test]
fn parse_args_rejects_a_third_positional() {
    let e = parse_args(&argv(&["/data", "/sock", "/extra"]), None).err();
    assert_eq!(e.as_deref(), Some("unexpected argument \"/extra\""));
    assert!(parse_args(&argv(&["/data"]), None).is_err());
}

#[test]
fn parse_args_rejects_a_bad_level() {
    assert!(parse_args(&argv(&["--log-level=loud", "/data", "/sock"]), None).is_err());
    assert!(parse_args(&argv(&["/data", "/sock"]), Some("loud")).is_err());
    let args = parse_args(&argv(&["--log-level=debug", "/data", "/sock"]), Some("quiet")).unwrap();
    assert_eq!(
        args.level,
        gnitz_foundation::log::DEBUG,
        "the flag overrides the environment"
    );
}

#[test]
fn parse_args_collects_the_tls_flags() {
    let args = parse_args(
        &argv(&[
            "/data",
            "--tls-listen=127.0.0.1:0",
            "--tls-cert=c.pem",
            "--tls-key=k.pem",
            "--tls-client-ca=ca.pem",
            "--allow-unauthenticated",
            "/sock",
            "--workers=3",
        ]),
        None,
    )
    .unwrap();
    assert_eq!(
        (args.data_dir.as_str(), args.socket_path.as_str(), args.workers),
        ("/data", "/sock", 3)
    );
    let tls = args.tls;
    assert_eq!(tls.listen, Some("127.0.0.1:0".parse().unwrap()));
    assert_eq!(tls.cert.as_deref(), Some("c.pem"));
    assert_eq!(tls.key.as_deref(), Some("k.pem"));
    assert_eq!(tls.client_ca.as_deref(), Some("ca.pem"));
    assert!(tls.allow_unauthenticated);
}
