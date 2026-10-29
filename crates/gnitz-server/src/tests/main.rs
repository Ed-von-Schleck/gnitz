use gnitz_wire::MAX_WORKERS;
use rustls::pki_types::ServerName;

use super::{parse_args, Args, Level};
use crate::runtime::CertSource;

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
        (
            "/data /sock",
            Some("loud"),
            "GNITZ_LOG_LEVEL: invalid log level \"loud\"",
        ),
        ("--workers=0 /data /sock", None, "--workers"),
        (too_many.as_str(), None, "--workers"),
        ("--workers=abc /data /sock", None, "--workers"),
        ("--workers=Auto /data /sock", None, "--workers"),
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
        ("--auth-tokens=t /data /sock", None, "--tls-listen"),
        ("--tls-name=db /data /sock", None, "--tls-listen"),
        (
            "--tls-listen=127.0.0.1:0 --tls-name=a,,b /data /sock",
            None,
            "--tls-name \"\"",
        ),
        (
            "--tls-listen=127.0.0.1:0 --tls-name=a..b /data /sock",
            None,
            "--tls-name \"a..b\"",
        ),
        (
            "--tls-listen=127.0.0.1:0 --tls-name=db --tls-cert=c.pem --tls-key=k.pem /data /sock",
            None,
            "an operator certificate",
        ),
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
fn the_worker_count_defaults_to_one_and_reaches_max_workers_or_auto() {
    for (flag, workers) in [
        (String::new(), Some(1)),
        ("--workers=1".to_string(), Some(1)),
        (format!("--workers={MAX_WORKERS}"), Some(MAX_WORKERS as u32)),
        ("--workers=auto".to_string(), None),
    ] {
        assert_eq!(
            parse(&format!("/data /sock {flag}"), None).unwrap().server.workers,
            workers
        );
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
    assert!(tls.cert == CertSource::Minted(Vec::new()) && !tls.allow_unauthenticated);
    assert!(tls.client_ca.is_none() && tls.auth_tokens.is_none());

    let line = "--tls-name=db.internal,10.0.0.5 /data --tls-listen=127.0.0.1:0 --tls-name=db2 --auth-tokens=tok \
                /sock";
    let tls = parse(line, None).unwrap().tls.unwrap();
    let names = ["db.internal", "10.0.0.5", "db2"].map(|n| ServerName::try_from(n).unwrap());
    assert_eq!(tls.cert, CertSource::Minted(names.to_vec()));
    assert_eq!(tls.auth_tokens.as_deref(), Some("tok"));

    let line = "/data --tls-listen=127.0.0.1:0 --tls-cert=c.pem --tls-key=k.pem \
                --tls-client-ca=ca.pem --allow-unauthenticated /sock";
    let args = parse(line, None).unwrap();
    assert_eq!(
        (args.server.data_dir.as_str(), args.server.socket_path.as_str()),
        ("/data", "/sock")
    );
    let tls = args.tls.unwrap();
    let operator = CertSource::Operator {
        cert: "c.pem".to_string(),
        key: "k.pem".to_string(),
    };
    assert_eq!(tls.cert, operator);
    assert_eq!(tls.client_ca.as_deref(), Some("ca.pem"));
    assert!(tls.allow_unauthenticated);
}

/// Every flag set the TLS bind policy refuses, or that names a file that does
/// not load, is refused naming what it refuses.
#[test]
fn every_tls_resolution_refusal_names_what_it_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let file = |name: &str, body: &str| {
        let path = dir.path().join(name);
        std::fs::write(&path, body).unwrap();
        path.to_str().unwrap().to_string()
    };
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert = file("cert.pem", &ck.cert.pem());
    let key = file("key.pem", &ck.signing_key.serialize_pem());
    let other_key = file("other_key.pem", &rcgen::KeyPair::generate().unwrap().serialize_pem());
    let bad_pem = format!(
        "{}-----BEGIN CERTIFICATE-----\n!!!\n-----END CERTIFICATE-----\n",
        ck.cert.pem()
    );
    let malformed = file("malformed.pem", &bad_pem);
    let empty = file("empty.pem", "");
    let tokens = file("tokens", "app1 nothex\n");
    let lo = "--tls-listen=127.0.0.1:0";
    let (no_auth, unnamed) = ("without client authentication", "without a certificate naming it");
    for (flags, names) in [
        ("--tls-listen=0.0.0.0:0".to_string(), no_auth),
        ("--tls-listen=[::]:0".to_string(), no_auth),
        ("--tls-listen=[::ffff:127.0.0.1]:0".to_string(), no_auth),
        ("--tls-listen=192.0.2.1:0 --allow-unauthenticated".to_string(), unnamed),
        (
            "--tls-listen=[2001:db8::1]:0 --allow-unauthenticated".to_string(),
            unnamed,
        ),
        (
            format!("{lo} --tls-cert=/nonexistent/c.pem --tls-key={key}"),
            "/nonexistent/c.pem",
        ),
        (
            format!("{lo} --tls-cert={cert} --tls-key=/nonexistent/k.pem"),
            "/nonexistent/k.pem",
        ),
        (format!("{lo} --tls-cert={empty} --tls-key={key}"), &empty),
        (
            format!("{lo} --tls-cert={cert} --tls-key={other_key}"),
            "cert/key rejected",
        ),
        (
            format!("{lo} --tls-client-ca=/nonexistent/ca.pem"),
            "/nonexistent/ca.pem",
        ),
        (format!("{lo} --tls-client-ca={malformed}"), &malformed),
        (format!("{lo} --tls-client-ca={empty}"), &empty),
        (
            format!("{lo} --tls-cert={cert} --tls-key={key} --tls-client-ca={empty}"),
            &empty,
        ),
        (format!("{lo} --auth-tokens={tokens}"), &tokens),
    ] {
        let tls = parse(&format!("{flags} /data /sock"), None).unwrap().tls.unwrap();
        let e = tls.resolve().err().unwrap_or_else(|| panic!("{flags:?} is refused"));
        assert!(e.contains(names), "{flags:?}: {e}");
    }
}
