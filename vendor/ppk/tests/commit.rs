use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use tempfile::TempDir;

/// Build a directory tree from (relative path, content) pairs.
fn tree(files: &[(&str, &[u8])]) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    for (path, content) in files {
        let path = dir.path().join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    }
    dir
}

fn commit_as(
    name: &str,
    version: &str,
    arch: &str,
    input: &Path,
    out: &Path,
) -> anyhow::Result<ppk::Commit> {
    let pkg = ppk::Package {
        name,
        version,
        arch,
    };
    ppk::commit(&pkg, input, out)
}

/// Commit `input` as hello 1.0 x86_64.
fn commit(input: &Path, out: &Path) -> anyhow::Result<ppk::Commit> {
    commit_as("hello", "1.0", "x86_64", input, out)
}

fn parse(manifest: &Path) -> toml::Table {
    toml::from_str(&fs::read_to_string(manifest).unwrap()).unwrap()
}

fn parse_files(manifest: &Path) -> toml::Table {
    let mut doc = parse(manifest);
    let keys: Vec<&String> = doc.keys().collect();
    assert_eq!(keys, ["arch", "files", "name", "version"]);
    let files = match doc.remove("files").unwrap() {
        toml::Value::Table(files) => files,
        other => panic!("[files] is not a table: {other:?}"),
    };
    for (path, entry) in &files {
        let entry = entry
            .as_table()
            .unwrap_or_else(|| panic!("{path}: not a table"));
        let keys: Vec<&String> = entry.keys().collect();
        assert!(
            keys == ["blob"] || keys == ["blob", "exec"] || keys == ["symlink"] || keys == ["dir"],
            "{path}: entry has keys {keys:?}"
        );
        for flag in ["exec", "dir"] {
            if let Some(value) = entry.get(flag) {
                assert_eq!(
                    value.as_bool(),
                    Some(true),
                    "{path}: {flag} is only ever true"
                );
            }
        }
    }
    files
}

fn blob_path(out: &Path, entry: &toml::Value) -> PathBuf {
    let hex = entry["blob"]
        .as_str()
        .unwrap()
        .strip_prefix("sha256:")
        .unwrap();
    out.join("blobs/sha256").join(format!("{hex}.zst"))
}

#[test]
fn round_trips_every_file() {
    let files: &[(&str, &[u8])] = &[
        ("bin/hello", b"\x7fELF pretend binary"),
        ("share/man/man1/hello.1", b".TH HELLO 1\n"),
        ("share/licenses/hello/COPYING", b"GPL text"),
        ("empty", b""),
    ];
    let input = tree(files);
    let out = tempfile::tempdir().unwrap();

    let commit = commit(input.path(), out.path()).unwrap();
    assert_eq!(commit.entries, files.len());
    assert_eq!(commit.blobs_written, files.len());

    let manifest = parse_files(&commit.manifest_path);
    assert_eq!(manifest.len(), files.len());
    for (path, content) in files {
        let blob = blob_path(out.path(), &manifest[*path]);
        let decoded = zstd::decode_all(fs::File::open(&blob).unwrap()).unwrap();
        assert_eq!(&decoded, content, "{path}");
        let name = blob.file_name().unwrap().to_str().unwrap();
        assert_eq!(name, format!("{:x}.zst", Sha256::digest(content)), "{path}");
    }
}

#[test]
fn manifest_is_filed_by_name_version_and_arch() {
    let input = tree(&[("a", b"a")]);
    let out = tempfile::tempdir().unwrap();
    let commit = commit_as(
        "org.gnu.hello",
        "2.12-1",
        "aarch64",
        input.path(),
        out.path(),
    )
    .unwrap();
    assert_eq!(
        commit.manifest_path,
        out.path()
            .join("manifests/org.gnu.hello/2.12-1/aarch64/manifest.toml")
    );
    let doc = parse(&commit.manifest_path);
    assert_eq!(doc["name"].as_str(), Some("org.gnu.hello"));
    assert_eq!(doc["version"].as_str(), Some("2.12-1"));
    assert_eq!(doc["arch"].as_str(), Some("aarch64"));
    assert_eq!(
        fs::read_to_string(&commit.manifest_path).unwrap(),
        "name = \"org.gnu.hello\"\n\
         version = \"2.12-1\"\n\
         arch = \"aarch64\"\n\
         \n\
         [files]\n\
         \"a\" = { blob = \"sha256:ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb\" }\n"
    );
}

#[test]
fn every_kind_of_entry_has_one_spelling() {
    let input = tree(&[("file", b"a"), ("tool", b"a")]);
    set_mode(&input.path().join("tool"), 0o755);
    std::os::unix::fs::symlink("file", input.path().join("link")).unwrap();
    fs::create_dir(input.path().join("empty")).unwrap();
    let out = tempfile::tempdir().unwrap();
    let commit = commit(input.path(), out.path()).unwrap();
    let text = fs::read_to_string(&commit.manifest_path).unwrap();
    let files: Vec<&str> = text.lines().skip_while(|l| *l != "[files]").collect();
    assert_eq!(
        files,
        [
            "[files]",
            "\"empty\" = { dir = true }",
            "\"file\" = { blob = \"sha256:ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb\" }",
            "\"link\" = { symlink = \"file\" }",
            "\"tool\" = { blob = \"sha256:ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb\", exec = true }",
        ]
    );
}

#[test]
fn manifest_hash_is_hash_of_manifest_bytes() {
    let input = tree(&[("a", b"a")]);
    let out = tempfile::tempdir().unwrap();
    let commit = commit(input.path(), out.path()).unwrap();
    let bytes = fs::read(&commit.manifest_path).unwrap();
    assert_eq!(
        commit.manifest_hash,
        format!("sha256:{:x}", Sha256::digest(&bytes))
    );
}

#[test]
fn identical_trees_give_identical_manifests() {
    // Created in different orders so directory iteration order differs.
    let a = tree(&[("z", b"1"), ("a/b", b"2"), ("m", b"3")]);
    let b = tree(&[("m", b"3"), ("a/b", b"2"), ("z", b"1")]);
    let out_a = tempfile::tempdir().unwrap();
    let out_b = tempfile::tempdir().unwrap();

    let ca = commit(a.path(), out_a.path()).unwrap();
    let cb = commit(b.path(), out_b.path()).unwrap();
    assert_eq!(ca.manifest_hash, cb.manifest_hash);
    assert_eq!(
        fs::read(&ca.manifest_path).unwrap(),
        fs::read(&cb.manifest_path).unwrap()
    );
}

#[test]
fn name_version_and_arch_are_part_of_identity() {
    let input = tree(&[("a", b"a")]);
    let out = tempfile::tempdir().unwrap();
    let hashes: Vec<String> = [
        ("hello", "1.0", "x86_64"),
        ("hello", "2.0", "x86_64"),
        ("goodbye", "1.0", "x86_64"),
        ("hello", "1.0", "aarch64"),
    ]
    .iter()
    .map(|(n, v, a)| {
        commit_as(n, v, a, input.path(), out.path())
            .unwrap()
            .manifest_hash
    })
    .collect();
    for (i, a) in hashes.iter().enumerate() {
        for b in &hashes[i + 1..] {
            assert_ne!(a, b);
        }
    }
}

#[test]
fn entries_are_sorted_by_bytes() {
    let input = tree(&[
        ("b", b""),
        ("a/z", b""),
        ("B", b""),
        ("a.txt", b""),
        ("é", b""),
    ]);
    let out = tempfile::tempdir().unwrap();
    let commit = commit(input.path(), out.path()).unwrap();
    let text = fs::read_to_string(&commit.manifest_path).unwrap();
    let keys: Vec<&str> = text
        .lines()
        .skip_while(|line| *line != "[files]")
        .skip(1)
        .map(|line| line.split(" = ").next().unwrap())
        .collect();
    assert_eq!(keys, ["\"B\"", "\"a.txt\"", "\"a/z\"", "\"b\"", "\"é\""]);
}

#[test]
fn identical_content_shares_one_blob() {
    let input = tree(&[("one", b"same"), ("two", b"same"), ("three", b"different")]);
    let out = tempfile::tempdir().unwrap();
    let commit = commit(input.path(), out.path()).unwrap();
    assert_eq!(commit.entries, 3);
    assert_eq!(commit.blobs_written, 2);
    assert_eq!(
        fs::read_dir(out.path().join("blobs/sha256"))
            .unwrap()
            .count(),
        2
    );
}

#[test]
fn versions_coexist_and_share_unchanged_blobs() {
    let v1 = tree(&[("bin/hello", b"v1 binary"), ("COPYING", b"license")]);
    let v2 = tree(&[("bin/hello", b"v2 binary"), ("COPYING", b"license")]);
    let out = tempfile::tempdir().unwrap();

    let c1 = commit_as("hello", "1.0", "x86_64", v1.path(), out.path()).unwrap();
    let c2 = commit_as("hello", "2.0", "x86_64", v2.path(), out.path()).unwrap();
    assert_eq!(c1.blobs_written, 2);
    assert_eq!(c2.blobs_written, 1, "COPYING is already stored");
    assert!(c1.manifest_path.exists());
    assert!(c2.manifest_path.exists());
    assert_ne!(c1.manifest_path, c2.manifest_path);
}

#[test]
fn recommitting_an_identical_version_succeeds() {
    let input = tree(&[("a", b"a")]);
    let out = tempfile::tempdir().unwrap();
    let first = commit(input.path(), out.path()).unwrap();
    let again = commit(input.path(), out.path()).unwrap();
    assert_eq!(again.blobs_written, 0);
    assert_eq!(again.manifest_hash, first.manifest_hash);
}

#[test]
fn a_committed_version_cannot_be_replaced() {
    let out = tempfile::tempdir().unwrap();
    let original = commit(tree(&[("a", b"original")]).path(), out.path()).unwrap();
    let before = fs::read(&original.manifest_path).unwrap();

    let err = commit(tree(&[("a", b"changed")]).path(), out.path()).unwrap_err();
    assert!(err.to_string().contains("already committed"), "{err:#}");
    assert_eq!(fs::read(&original.manifest_path).unwrap(), before);
    let leftovers: Vec<_> = fs::read_dir(original.manifest_path.parent().unwrap())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(
        leftovers,
        ["manifest.toml"],
        "no temporary files left behind"
    );
}

#[test]
fn arches_of_one_version_coexist() {
    let x86 = tree(&[("bin/hello", b"x86 binary")]);
    let arm = tree(&[("bin/hello", b"arm binary")]);
    let out = tempfile::tempdir().unwrap();
    let a = commit_as("hello", "1.0", "x86_64", x86.path(), out.path()).unwrap();
    let b = commit_as("hello", "1.0", "aarch64", arm.path(), out.path()).unwrap();
    assert!(a.manifest_path.exists());
    assert!(b.manifest_path.exists());
    assert_ne!(a.manifest_path, b.manifest_path);
}

#[test]
fn unsafe_names_versions_and_arches_are_rejected() {
    let input = tree(&[("a", b"a")]);
    let out = tempfile::tempdir().unwrap();
    for bad in [
        "",
        ".",
        "..",
        "../escape",
        "a/b",
        ".hidden",
        "-flag",
        "sp ace",
        "Ünï",
    ] {
        let err = commit_as(bad, "1.0", "x86_64", input.path(), out.path()).unwrap_err();
        assert!(
            err.to_string().contains("invalid package name"),
            "{bad:?}: {err:#}"
        );
        let err = commit_as("hello", bad, "x86_64", input.path(), out.path()).unwrap_err();
        assert!(
            err.to_string().contains("invalid version"),
            "{bad:?}: {err:#}"
        );
        let err = commit_as("hello", "1.0", bad, input.path(), out.path()).unwrap_err();
        assert!(err.to_string().contains("invalid arch"), "{bad:?}: {err:#}");
    }
    assert_eq!(
        fs::read_dir(out.path()).unwrap().count(),
        0,
        "nothing written"
    );
}

#[test]
fn awkward_names_are_escaped_and_parse_back() {
    let names = [
        "quote\"d",
        "back\\slash",
        "tab\there",
        "new\nline",
        "del\x7f",
        "ünïcödé",
    ];
    let files: Vec<(&str, &[u8])> = names.iter().map(|n| (*n, n.as_bytes())).collect();
    let input = tree(&files);
    let out = tempfile::tempdir().unwrap();
    let commit = commit(input.path(), out.path()).unwrap();

    let text = fs::read_to_string(&commit.manifest_path).unwrap();
    assert!(text.contains(r#""quote\"d""#));
    assert!(text.contains(r#""back\\slash""#));
    assert!(text.contains(r#""tab\u0009here""#));
    assert!(text.contains(r#""new\u000Aline""#));
    assert!(text.contains(r#""del\u007F""#));
    assert!(text.contains("\"ünïcödé\""));

    let manifest = parse_files(&commit.manifest_path);
    for name in names {
        let blob = blob_path(out.path(), &manifest[name]);
        let decoded = zstd::decode_all(fs::File::open(blob).unwrap()).unwrap();
        assert_eq!(decoded, name.as_bytes());
    }
}

#[test]
fn empty_content_gives_empty_files_table() {
    let input = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let commit = commit(input.path(), out.path()).unwrap();
    assert_eq!(
        fs::read_to_string(&commit.manifest_path).unwrap(),
        "name = \"hello\"\nversion = \"1.0\"\narch = \"x86_64\"\n\n[files]\n"
    );
    assert_eq!(commit.entries, 0);
}

#[test]
fn only_empty_directories_are_listed() {
    let input = tree(&[("usr/bin/hello", b"x")]);
    fs::create_dir_all(input.path().join("var/lib/hello")).unwrap();
    fs::create_dir_all(input.path().join("var/cache/hello/nested")).unwrap();
    fs::create_dir_all(input.path().join("etc/links")).unwrap();
    std::os::unix::fs::symlink("x", input.path().join("etc/links/only-a-link")).unwrap();
    let out = tempfile::tempdir().unwrap();

    let commit = commit(input.path(), out.path()).unwrap();
    let manifest = parse_files(&commit.manifest_path);
    let paths: Vec<&String> = manifest.keys().collect();
    assert_eq!(
        paths,
        [
            "etc/links/only-a-link",
            "usr/bin/hello",
            "var/cache/hello/nested",
            "var/lib/hello",
        ],
        "directories with contents are implied, not listed"
    );
    assert_eq!(manifest["var/lib/hello"]["dir"].as_bool(), Some(true));
    assert_eq!(
        manifest["var/cache/hello/nested"]["dir"].as_bool(),
        Some(true)
    );
}

fn set_mode(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
}

#[test]
fn any_execute_bit_marks_a_file_exec() {
    let modes = [
        ("rw", 0o644, false),
        ("rwx", 0o755, true),
        ("owner-only", 0o700, true),
        ("group-only", 0o610, true),
        ("other-only", 0o601, true),
        ("read-only", 0o444, false),
        ("sticky", 0o1644, false),
    ];
    let files: Vec<(&str, &[u8])> = modes.iter().map(|(n, _, _)| (*n, &b"x"[..])).collect();
    let input = tree(&files);
    for (name, mode, _) in modes {
        set_mode(&input.path().join(name), mode);
    }
    let out = tempfile::tempdir().unwrap();
    let commit = commit(input.path(), out.path()).unwrap();
    assert_eq!(commit.blobs_written, 1, "mode never affects the blob");
    let manifest = parse_files(&commit.manifest_path);
    for (name, mode, exec) in modes {
        let recorded = manifest[name].get("exec").is_some();
        assert_eq!(recorded, exec, "{name} ({mode:o})");
    }
}

#[test]
fn exec_bit_is_part_of_identity_but_not_of_the_blob() {
    let input = tree(&[("bin/hello", b"binary")]);
    let out = tempfile::tempdir().unwrap();
    let plain = commit_as("hello", "1.0", "x86_64", input.path(), out.path()).unwrap();
    set_mode(&input.path().join("bin/hello"), 0o755);
    let exec = commit_as("hello", "1.1", "x86_64", input.path(), out.path()).unwrap();
    assert_eq!(exec.blobs_written, 0);
    assert_ne!(plain.manifest_hash, exec.manifest_hash);
}

#[test]
fn setuid_and_setgid_files_are_rejected() {
    for mode in [0o4755, 0o2755] {
        let input = tree(&[("bin/su", b"x")]);
        set_mode(&input.path().join("bin/su"), mode);
        let out = tempfile::tempdir().unwrap();
        let err = commit(input.path(), out.path()).unwrap_err();
        assert!(
            err.to_string()
                .contains("setuid and setgid bits are not supported"),
            "{mode:o}: {err:#}"
        );
    }
}

#[test]
fn symlink_targets_are_recorded_verbatim() {
    use std::os::unix::fs::symlink;
    let input = tree(&[("usr/lib/libz.so.1.3", b"lib")]);
    let lib = input.path().join("usr/lib");
    symlink("libz.so.1.3", lib.join("libz.so")).unwrap();
    symlink("/usr/share/zoneinfo/UTC", input.path().join("localtime")).unwrap();
    symlink("../lib/./libz.so.1.3", lib.join("unnormalised")).unwrap();
    symlink("nowhere/at/all", lib.join("dangling")).unwrap();
    let out = tempfile::tempdir().unwrap();

    let commit = commit(input.path(), out.path()).unwrap();
    assert_eq!(commit.entries, 5);
    assert_eq!(commit.blobs_written, 1, "symlinks have no blobs");
    let manifest = parse_files(&commit.manifest_path);
    for (path, target) in [
        ("usr/lib/libz.so", "libz.so.1.3"),
        ("localtime", "/usr/share/zoneinfo/UTC"),
        ("usr/lib/unnormalised", "../lib/./libz.so.1.3"),
        ("usr/lib/dangling", "nowhere/at/all"),
    ] {
        assert_eq!(manifest[path]["symlink"].as_str(), Some(target), "{path}");
    }
}

#[test]
fn symlinked_directories_are_not_followed() {
    let input = tree(&[("real/file", b"x")]);
    std::os::unix::fs::symlink("real", input.path().join("alias")).unwrap();
    let out = tempfile::tempdir().unwrap();
    let commit = commit(input.path(), out.path()).unwrap();
    let manifest = parse_files(&commit.manifest_path);
    let paths: Vec<&String> = manifest.keys().collect();
    assert_eq!(paths, ["alias", "real/file"]);
    assert_eq!(manifest["alias"]["symlink"].as_str(), Some("real"));
}

#[test]
fn awkward_symlink_targets_are_escaped_and_parse_back() {
    let input = tempfile::tempdir().unwrap();
    let target = "we\"ird\\tar\nget ü";
    std::os::unix::fs::symlink(target, input.path().join("link")).unwrap();
    let out = tempfile::tempdir().unwrap();
    let commit = commit(input.path(), out.path()).unwrap();
    let text = fs::read_to_string(&commit.manifest_path).unwrap();
    assert!(
        text.contains(r#"{ symlink = "we\"ird\\tar\u000Aget ü" }"#),
        "{text}"
    );
    let manifest = parse_files(&commit.manifest_path);
    assert_eq!(manifest["link"]["symlink"].as_str(), Some(target));
}

#[test]
fn non_utf8_symlink_targets_are_rejected() {
    use std::os::unix::ffi::OsStrExt;
    let input = tempfile::tempdir().unwrap();
    let target = std::ffi::OsStr::from_bytes(b"bad\xff");
    std::os::unix::fs::symlink(target, input.path().join("link")).unwrap();
    let out = tempfile::tempdir().unwrap();
    let err = commit(input.path(), out.path()).unwrap_err();
    assert!(
        err.to_string()
            .contains("symlink target is not valid UTF-8"),
        "{err:#}"
    );
}

#[test]
fn special_files_are_rejected() {
    let input = tempfile::tempdir().unwrap();
    let _socket = std::os::unix::net::UnixListener::bind(input.path().join("sock")).unwrap();
    let out = tempfile::tempdir().unwrap();
    let err = commit(input.path(), out.path()).unwrap_err();
    assert!(err.to_string().contains("not a regular file"), "{err:#}");
}

#[test]
fn non_utf8_paths_are_rejected() {
    use std::os::unix::ffi::OsStrExt;
    let input = tempfile::tempdir().unwrap();
    let name = std::ffi::OsStr::from_bytes(b"bad\xff");
    fs::write(input.path().join(name), b"x").unwrap();
    let out = tempfile::tempdir().unwrap();
    let err = commit(input.path(), out.path()).unwrap_err();
    assert!(err.to_string().contains("not valid UTF-8"), "{err:#}");
}

#[test]
fn output_inside_input_is_rejected() {
    let input = tree(&[("a", b"a")]);
    let out = input.path().join("not/yet/created");
    let err = commit(input.path(), &out).unwrap_err();
    assert!(err.to_string().contains("inside the input"), "{err:#}");
    assert!(!input.path().join("not").exists());
}

#[test]
fn input_must_be_a_directory() {
    let input = tree(&[("file", b"x")]);
    let out = tempfile::tempdir().unwrap();
    let err = commit(&input.path().join("file"), out.path()).unwrap_err();
    assert!(err.to_string().contains("not a directory"), "{err:#}");
}
