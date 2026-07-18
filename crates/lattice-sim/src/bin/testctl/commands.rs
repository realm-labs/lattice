use std::process::Command;

pub(super) fn cargo(arguments: &[&str]) -> Result<(), String> {
    command("cargo", arguments)
}

pub(super) fn cargo_test_exact(package: &str, name: &str) -> Result<(), String> {
    let listed = output(
        "cargo",
        &[
            "test", "--lib", "-p", package, name, "--", "--exact", "--list",
        ],
    )?;
    let expected = format!("{name}: test");
    if !listed.lines().any(|line| line.trim() == expected) {
        return Err(format!(
            "exact test {name} was not discovered in package {package}"
        ));
    }
    cargo(&["test", "--lib", "-p", package, name, "--", "--exact"])
}

pub(super) fn command(program: &str, arguments: &[&str]) -> Result<(), String> {
    let status = Command::new(program)
        .args(arguments)
        .status()
        .map_err(|error| format!("failed to start {program}: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{program} exited with {status}"))
    }
}

pub(super) fn output(program: &str, arguments: &[&str]) -> Result<String, String> {
    let output = Command::new(program)
        .args(arguments)
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "{program} exited with {}: {}",
            output.status,
            stderr.trim()
        ));
    }
    String::from_utf8(output.stdout)
        .map(|value| value.trim().to_owned())
        .map_err(|error| error.to_string())
}
