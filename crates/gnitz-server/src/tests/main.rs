use super::{parse_args, Args, Level};
use crate::runtime::MAX_WORKERS;

/// `line`, split on whitespace, as argv.
fn parse(line: &str, env_level: Option<&str>) -> Result<Args, String> {
    let argv: Vec<String> = line.split_whitespace().map(String::from).collect();
    parse_args(&argv, env_level)
}

#[test]
fn every_refusal_names_what_it_refuses() {
    let too_many = format!("--workers={} /data /sock", MAX_WORKERS + 1);
    for (line, env_level, names) in [
        ("--tls-lsten=127.0.0.1:0 /data /sock", None, "--tls-lsten"),
        ("--workers /data /sock", None, "\"--workers\""),
        ("/data /sock /extra", None, "/extra"),
        ("/data", None, "<socket_path>"),
        ("", None, "<data_dir>"),
        ("--log-level=loud /data /sock", None, "loud"),
        ("/data /sock", Some("loud"), "loud"),
        ("--workers=0 /data /sock", None, "--workers"),
        (too_many.as_str(), None, "--workers"),
        ("--workers=abc /data /sock", None, "--workers"),
        ("--tls-listen=localhost:0 /data /sock", None, "localhost:0"),
        (
            "--tls-listen=127.0.0.1:0 --tls-cert=c.pem /data /sock",
            None,
            "--tls-key",
        ),
        (
            "--tls-listen=127.0.0.1:0 --tls-key=k.pem /data /sock",
            None,
            "--tls-cert",
        ),
        ("--tls-cert=c.pem /data /sock", None, "--tls-listen"),
        ("--tls-key=k.pem /data /sock", None, "--tls-listen"),
        ("--tls-client-ca=ca.pem /data /sock", None, "--tls-listen"),
        ("--allow-unauthenticated /data /sock", None, "--tls-listen"),
    ] {
        let e = parse(line, env_level)
            .err()
            .unwrap_or_else(|| panic!("{line:?} is refused"));
        assert!(e.contains(names), "{line:?}: {e}");
    }
}

#[test]
fn the_level_is_the_flag_else_the_environment_else_quiet() {
    for (flag, env_level, level) in [
        ("", None, Level::Quiet),
        ("", Some("normal"), Level::Normal),
        ("", Some("verbose"), Level::Debug),
        ("--log-level=quiet", Some("debug"), Level::Quiet),
        ("--log-level=debug", Some("quiet"), Level::Debug),
    ] {
        let args = parse(&format!("/data /sock {flag}"), env_level).unwrap();
        assert_eq!(args.level, level, "{flag:?} under {env_level:?}");
    }
}

#[test]
fn the_worker_count_defaults_to_one_and_reaches_max_workers() {
    for (flag, workers) in [
        (String::new(), 1),
        ("--workers=1".to_string(), 1),
        (format!("--workers={MAX_WORKERS}"), MAX_WORKERS as u32),
    ] {
        assert_eq!(parse(&format!("/data /sock {flag}"), None).unwrap().workers, workers);
    }
}

#[test]
fn the_tls_flags_are_collected_wherever_they_stand() {
    assert!(
        parse("/data /sock", None).unwrap().tls.is_none(),
        "no --tls-listen, no listener"
    );

    let tls = parse("--tls-listen=[::1]:7 /data /sock", None).unwrap().tls.unwrap();
    assert_eq!(tls.listen, "[::1]:7".parse().unwrap());
    assert!(tls.cert_key.is_none() && tls.client_ca.is_none() && !tls.allow_unauthenticated);

    let line = "/data --tls-listen=127.0.0.1:0 --tls-cert=c.pem --tls-key=k.pem \
                --tls-client-ca=ca.pem --allow-unauthenticated /sock";
    let args = parse(line, None).unwrap();
    assert_eq!((args.data_dir.as_str(), args.socket_path.as_str()), ("/data", "/sock"));
    let tls = args.tls.unwrap();
    assert_eq!(tls.cert_key, Some(("c.pem".to_string(), "k.pem".to_string())));
    assert_eq!(tls.client_ca.as_deref(), Some("ca.pem"));
    assert!(tls.allow_unauthenticated);
}
